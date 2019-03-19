package coop.rchain.rspace

import cats.effect.{Concurrent, ContextShift}
import cats.implicits._
import com.google.common.collect.Multiset
import com.typesafe.scalalogging.Logger
import coop.rchain.catscontrib.Catscontrib._
import coop.rchain.catscontrib._
import coop.rchain.metrics.{Metrics, Span}
import coop.rchain.rspace.history.Branch
import coop.rchain.rspace.internal._
import coop.rchain.rspace.trace.{Produce, _}
import coop.rchain.shared.Log
import scodec.Codec

import scala.collection.JavaConverters._
import scala.collection.immutable.Seq
import scala.concurrent.ExecutionContext

class ReplayRSpace[F[_], C, P, A, R, K](store: IStore[F, C, P, A, K], branch: Branch)(
    implicit
    serializeC: Serialize[C],
    serializeP: Serialize[P],
    serializeA: Serialize[A],
    serializeK: Serialize[K],
    val concurrent: Concurrent[F],
    logF: Log[F],
    contextShift: ContextShift[F],
    scheduler: ExecutionContext,
    metricsF: Metrics[F]
) extends RSpaceOps[F, C, P, A, R, K](store, branch)
    with IReplaySpace[F, C, P, A, R, K] {

  override protected[this] val logger: Logger = Logger[this.type]

  private[this] implicit val MetricsSource: Metrics.Source =
    Metrics.Source(RSpaceMetricsSource, "replay")

  private[this] val consumeCommLabel = "comm.consume"
  private[this] val produceCommLabel = "comm.produce"
  private[this] val consumeSpanLabel = Metrics.Source(MetricsSource, "consume")
  private[this] val produceSpanLabel = Metrics.Source(MetricsSource, "produce")

  def consume(
      channels: Seq[C],
      patterns: Seq[P],
      continuation: K,
      persist: Boolean,
      sequenceNumber: Int
  )(
      implicit m: Match[F, P, A, R]
  ): F[MaybeActionResult] =
    contextShift.evalOn(scheduler) {
      if (channels.length =!= patterns.length) {
        val msg = "channels.length must equal patterns.length"
        logF.error(msg) *> syncF.raiseError(new IllegalArgumentException(msg))
      } else
        for {
          span <- metricsF.span(consumeSpanLabel)
          _    <- span.mark("before-consume-lock")
          result <- consumeLockF(channels) {
                     lockedConsume(channels, patterns, continuation, persist, sequenceNumber, span)
                   }
          _ <- span.mark("post-consume-lock")
        } yield result
    }

  private[this] def lockedConsume(
      channels: Seq[C],
      patterns: Seq[P],
      continuation: K,
      persist: Boolean,
      sequenceNumber: Int,
      span: Span[F]
  )(
      implicit m: Match[F, P, A, R]
  ): F[MaybeActionResult] = {
    def runMatcher(comm: COMM): F[Option[Seq[DataCandidate[C, R]]]] =
      for {
        channelToIndexedDataList <- channels.toList.traverse { c: C =>
                                     store
                                       .withReadTxnF { txn =>
                                         store.getData(txn, Seq(c)).zipWithIndex.filter {
                                           case (Datum(_, _, source), _) =>
                                             comm.produces.contains(source)
                                         }
                                       }
                                       .map(v => c -> v) // TODO inculde map in traverse?
                                   }
        result <- extractDataCandidates(channels.zip(patterns), channelToIndexedDataList.toMap, Nil)
                   .map(_.sequence)
      } yield result

    def storeWaitingContinuation(
        consumeRef: Consume,
        maybeCommRef: Option[COMM]
    ): F[MaybeActionResult] =
      for {
        _ <- store
              .withWriteTxnF { txn =>
                store.putWaitingContinuation(
                  txn,
                  channels,
                  WaitingContinuation(patterns, continuation, persist, consumeRef)
                )
                for (channel <- channels) store.addJoin(txn, channel, channels)
              }
        _ <- logF.debug(s"""|consume: no data found,
                            |storing <(patterns, continuation): ($patterns, $continuation)>
                            |at <channels: $channels>""".stripMargin.replace('\n', ' '))
      } yield None

    def handleMatches(
        mats: Seq[DataCandidate[C, R]],
        consumeRef: Consume,
        comms: Multiset[COMM]
    ): F[MaybeActionResult] =
      for {
        _       <- metricsF.incrementCounter(consumeCommLabel)
        commRef <- syncF.delay { COMM(consumeRef, mats.map(_.datum.source)) }
        //fixme replace with raiseError
        _ = assert(comms.contains(commRef), "COMM Event was not contained in the trace")
        r <- mats.toList
              .sortBy(_.datumIndex)(Ordering[Int].reverse)
              .traverse {
                case DataCandidate(candidateChannel, Datum(_, persistData, _), dataIndex) =>
                  if (!persistData) {
                    store.withWriteTxnF { txn =>
                      store.removeDatum(txn, Seq(candidateChannel), dataIndex)
                    }
                  } else ().pure[F]
              }
              .flatMap { _ =>
                logF.debug(
                  s"consume: data found for <patterns: $patterns> at <channels: $channels>"
                )
              }
              .map { _ =>
                removeBindingsFor(commRef)
                val contSequenceNumber = commRef.nextSequenceNumber
                Some(
                  (
                    ContResult(continuation, persist, channels, patterns, contSequenceNumber),
                    mats.map(dc => Result(dc.datum.a, dc.datum.persist))
                  )
                )
              }
      } yield r

    @SuppressWarnings(Array("org.wartremover.warts.Throw"))
    // TODO stop throwing exceptions
    def getCommOrDataCandidates(comms: Seq[COMM]): F[Either[COMM, Seq[DataCandidate[C, R]]]] = {
      type COMMOrData = Either[COMM, Seq[DataCandidate[C, R]]]
      def go(comms: Seq[COMM]): F[Either[Seq[COMM], Either[COMM, Seq[DataCandidate[C, R]]]]] =
        comms match {
          case Nil =>
            val msg = "List comms must not be empty"
            //fixme replace with raiseError
            logger.error(msg)
            throw new IllegalArgumentException(msg)
          case commRef :: Nil =>
            runMatcher(commRef).map {
              case Some(x) => x.asRight[COMM].asRight[Seq[COMM]]
              case None    => commRef.asLeft[Seq[DataCandidate[C, R]]].asRight[Seq[COMM]]
            }
          case commRef :: rem =>
            runMatcher(commRef).map {
              case Some(x) => x.asRight[COMM].asRight[Seq[COMM]]
              case None    => rem.asLeft[COMMOrData]
            }
        }
      comms.tailRecM(go)
    }

    for {
      _ <- logF.debug(s"""|consume: searching for data matching <patterns: $patterns>
                          |at <channels: $channels>""".stripMargin.replace('\n', ' '))
      consumeRef <- syncF.delay {
                     Consume.create(channels, patterns, continuation, persist, sequenceNumber)
                   }
      _ <- span.mark("after-compute-consumeref")
      r <- replayData.get(consumeRef) match {
            case None =>
              storeWaitingContinuation(consumeRef, None)
            case Some(comms) =>
              val commOrDataCandidates: F[Either[COMM, Seq[DataCandidate[C, R]]]] =
                getCommOrDataCandidates(comms.iterator().asScala.toList)

              val x: F[MaybeActionResult] = commOrDataCandidates.flatMap {
                case Left(commRef) =>
                  storeWaitingContinuation(consumeRef, Some(commRef))
                case Right(dataCandidates) =>
                  handleMatches(dataCandidates, consumeRef, comms)
              }
              x
          }
    } yield r
  }

  def produce(channel: C, data: A, persist: Boolean, sequenceNumber: Int)(
      implicit m: Match[F, P, A, R]
  ): F[MaybeActionResult] =
    contextShift.evalOn(scheduler) {
      for {
        span <- metricsF.span(produceSpanLabel)
        _    <- span.mark("before-produce-lock")
        result <- produceLockF(channel) {
                   lockedProduce(channel, data, persist, sequenceNumber, span)
                 }
        _ <- span.mark("post-produce-lock")
        _ <- span.close()
      } yield result
    }

  private[this] def lockedProduce(
      channel: C,
      data: A,
      persist: Boolean,
      sequenceNumber: Int,
      span: Span[F]
  )(
      implicit m: Match[F, P, A, R]
  ): F[MaybeActionResult] = {

    type MaybeProduceCandidate = Option[ProduceCandidate[C, P, R, K]]

    def runMatcher(
        comm: COMM,
        produceRef: Produce,
        groupedChannels: Seq[Seq[C]]
    ): F[MaybeProduceCandidate] = {

      def go(groupedChannels: Seq[Seq[C]]): F[Either[Seq[Seq[C]], MaybeProduceCandidate]] =
        groupedChannels match {
          case Nil => none[ProduceCandidate[C, P, R, K]].asRight[Seq[Seq[C]]].pure[F]
          case channels :: remaining =>
            for {
              matchCandidates <- store.withReadTxnF { txn =>
                                  store.getWaitingContinuation(txn, channels).zipWithIndex.filter {
                                    case (WaitingContinuation(_, _, _, source), _) =>
                                      comm.consume == source
                                  }
                                }
              channelToIndexedDataList <- store.withReadTxnF { txn =>
                                           channels.map { c: C =>
                                             val as =
                                               store.getData(txn, Seq(c)).zipWithIndex.filter {
                                                 case (Datum(_, _, source), _) =>
                                                   comm.produces.contains(source)
                                               }
                                             c -> {
                                               if (c == channel)
                                                 Seq((Datum(data, persist, produceRef), -1))
                                               else as
                                             }
                                           }
                                         }
              firstMatch <- extractFirstMatch(
                             channels,
                             matchCandidates,
                             channelToIndexedDataList.toMap
                           )
            } yield
              firstMatch match {
                case None             => remaining.asLeft[MaybeProduceCandidate]
                case produceCandidate => produceCandidate.asRight[Seq[Seq[C]]]
              }
        }
      groupedChannels.tailRecM(go)
    }

    @SuppressWarnings(Array("org.wartremover.warts.Throw"))
    // TODO stop throwing exceptions
    def getCommOrProduceCandidate(
        comms: Seq[COMM],
        produceRef: Produce,
        groupedChannels: Seq[Seq[C]]
    ): F[Either[COMM, ProduceCandidate[C, P, R, K]]] = {
      type COMMOrProduce = Either[COMM, ProduceCandidate[C, P, R, K]]
      def go(comms: Seq[COMM]): F[Either[Seq[COMM], COMMOrProduce]] =
        comms match {
          case Nil =>
            val msg = "comms must not be empty"
            //fixme replace with raiseError
            logger.error(msg)
            throw new IllegalArgumentException(msg)
          case commRef :: Nil =>
            runMatcher(commRef, produceRef, groupedChannels) map {
              case Some(x) => x.asRight[COMM].asRight[Seq[COMM]]
              case None    => commRef.asLeft[ProduceCandidate[C, P, R, K]].asRight[Seq[COMM]]
            }
          case commRef :: rem =>
            runMatcher(commRef, produceRef, groupedChannels) map {
              case Some(x) => x.asRight[COMM].asRight[Seq[COMM]]
              case None    => rem.asLeft[COMMOrProduce]
            }
        }
      comms.tailRecM(go)
    }

    def storeDatum(
        produceRef: Produce,
        maybeCommRef: Option[COMM]
    ): F[MaybeActionResult] =
      for {
        _ <- store
              .withWriteTxnF { txn =>
                store.putDatum(txn, Seq(channel), Datum(data, persist, produceRef))
              }
        _ <- logF.debug(s"""|produce: no matching continuation found
                            |storing <data: $data> at <channel: $channel>""".stripMargin)
      } yield None

    def handleMatch(
        mat: ProduceCandidate[C, P, R, K],
        produceRef: Produce,
        comms: Multiset[COMM]
    ): F[MaybeActionResult] =
      mat match {
        case ProduceCandidate(
            channels,
            WaitingContinuation(patterns, continuation, persistK, consumeRef),
            continuationIndex,
            dataCandidates
            ) =>
          for {
            _       <- metricsF.incrementCounter(produceCommLabel)
            commRef <- syncF.delay { COMM(consumeRef, dataCandidates.map(_.datum.source)) }
            //fixme replace with raiseError
            _ = assert(comms.contains(commRef), "COMM Event was not contained in the trace")
            _ <- if (!persistK) {
                  store.withWriteTxnF { txn =>
                    store.removeWaitingContinuation(txn, channels, continuationIndex)
                  }
                } else {
                  ().pure[F]
                }
            _ <- dataCandidates.toList
                  .sortBy(_.datumIndex)(Ordering[Int].reverse)
                  .traverse {
                    case DataCandidate(candidateChannel, Datum(_, persistData, _), dataIndex) =>
                      store.withWriteTxnF { txn =>
                        if (!persistData && dataIndex >= 0) {
                          store.removeDatum(txn, Seq(candidateChannel), dataIndex)
                        }
                        store.removeJoin(txn, candidateChannel, channels)
                      }
                  }
            _ <- logF.debug(s"produce: matching continuation found at <channels: $channels>")
            r <- syncF.delay {
                  removeBindingsFor(commRef)
                  val contSequenceNumber = commRef.nextSequenceNumber
                  Some(
                    (
                      ContResult(continuation, persistK, channels, patterns, contSequenceNumber),
                      dataCandidates.map(dc => Result(dc.datum.a, dc.datum.persist))
                    )
                  )
                }
          } yield r
      }

    for {
      groupedChannels <- store.withReadTxnF { txn =>
                          store.getJoin(txn, channel)
                        }
      _ <- span.mark("after-fetch-joins")
      _ <- logF.debug(
            s"""|produce: searching for matching continuations
                |at <groupedChannels: $groupedChannels>""".stripMargin
              .replace('\n', ' ')
          )
      produceRef <- syncF.delay { Produce.create(channel, data, persist, sequenceNumber) }
      _          <- span.mark("after-compute-produceref")
      result <- replayData.get(produceRef) match {
                 case None =>
                   storeDatum(produceRef, None)
                 case Some(comms) =>
                   val commOrProduceCandidate: F[Either[COMM, ProduceCandidate[C, P, R, K]]] =
                     getCommOrProduceCandidate(
                       comms.iterator().asScala.toList,
                       produceRef,
                       groupedChannels
                     )
                   val r: F[MaybeActionResult] = commOrProduceCandidate.flatMap {
                     case Left(comm) =>
                       storeDatum(produceRef, Some(comm))
                     case Right(produceCandidate) =>
                       handleMatch(produceCandidate, produceRef, comms)
                   }
                   r
               }
    } yield result
  }

  @SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
  @inline
  private[this] def removeBindingsFor(
      commRef: COMM
  ): Unit =
    commRef.produces.foldLeft(replayData.removeBinding(commRef.consume, commRef)) {
      case (updatedReplays, produceRef) =>
        updatedReplays.removeBinding(produceRef, commRef)
    }

  def createCheckpoint(): F[Checkpoint] =
    contextShift.evalOn(scheduler) {
      for {
        isEmpty <- syncF.delay(replayData.isEmpty)
        checkpoint <- isEmpty.fold(
                       syncF.delay { Checkpoint(store.createCheckpoint(), Seq.empty) }, {
                         val msg =
                           s"unused comm event: replayData multimap has ${replayData.size} elements left"
                         logF.error(msg) *> syncF.raiseError[Checkpoint](new ReplayException(msg))
                       }
                     )
      } yield checkpoint
    }

  override def clear(): F[Unit] =
    for {
      _ <- syncF.delay {
            replayData.clear()
          }
      _ <- super.clear()
    } yield ()
}

object ReplayRSpace {

  def create[F[_], C, P, A, R, K](context: Context[F, C, P, A, K], branch: Branch)(
      implicit
      sc: Serialize[C],
      sp: Serialize[P],
      sa: Serialize[A],
      sk: Serialize[K],
      concurrent: Concurrent[F],
      log: Log[F],
      contextShift: ContextShift[F],
      scheduler: ExecutionContext,
      metricsF: Metrics[F]
  ): F[ReplayRSpace[F, C, P, A, R, K]] = {

    implicit val codecC: Codec[C] = sc.toCodec
    implicit val codecP: Codec[P] = sp.toCodec
    implicit val codecA: Codec[A] = sa.toCodec
    implicit val codecK: Codec[K] = sk.toCodec

    val mainStore: IStore[F, C, P, A, K] = context match {
      case lmdbContext: LMDBContext[F, C, P, A, K] =>
        LMDBStore.create[F, C, P, A, K](lmdbContext, branch)

      case memContext: InMemoryContext[F, C, P, A, K] =>
        InMemoryStore.create(memContext.trieStore, branch)

      case mixedContext: MixedContext[F, C, P, A, K] =>
        LockFreeInMemoryStore.create(mixedContext.trieStore, branch)
    }

    val replaySpace = new ReplayRSpace[F, C, P, A, R, K](mainStore, branch)

    /*
     * history.initialize returns true if the history trie contains no root (i.e. is empty).
     *
     * In this case, we create a checkpoint for the empty store so that we can reset
     * to the empty store state with the clear method.
     */
    if (history.initialize(mainStore.trieStore, branch)) {
      replaySpace.createCheckpoint().map(_ => replaySpace)
    } else {
      replaySpace.pure[F]
    }
  }

}
