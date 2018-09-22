package coop.rchain.casper.util.rholang

import cats.effect.{ExitCase, Sync}
import cats.implicits._
import com.google.protobuf.ByteString
import coop.rchain.casper.protocol._
import coop.rchain.casper.util.ProtoUtil
import coop.rchain.casper.util.rholang.RuntimeManager.StateHash
import coop.rchain.catscontrib.TaskContrib._
import coop.rchain.crypto.codec.Base16
import coop.rchain.crypto.hash.Blake2b512Random
import coop.rchain.models.Channel.ChannelInstance.Quote
import coop.rchain.models.Expr.ExprInstance.GString
import coop.rchain.models._
import coop.rchain.rholang.interpreter.accounting.{CostAccount, CostAccountingAlg}
import coop.rchain.rholang.interpreter.storage.StoragePrinter
import coop.rchain.rholang.interpreter.{ErrorLog, Reduce, Runtime}
import coop.rchain.rspace.internal.{Datum, WaitingContinuation}
import coop.rchain.rspace.trace.Produce
import coop.rchain.rspace.{Blake2b256Hash, ReplayException}
import monix.eval.Task
import monix.execution.Scheduler

import scala.annotation.tailrec
import scala.collection.immutable
import scala.concurrent.SyncVar
import scala.util.{Failure, Success, Try}

//runtime is a SyncVar for thread-safety, as all checkpoints share the same "hot store"
class RuntimeManager[F[_]: Sync] private (
    val emptyStateHash: ByteString,
    runtimeContainer: SyncVar[Runtime[F]]
) {

  def captureResults(start: StateHash, term: Par, name: String = "__SCALA__"): F[Seq[Par]] = {
    val result: F[immutable.Seq[Datum[ListChannelWithRandom]]] =
      Sync[F].bracket(runtimeContainer.take().pure[F])(runtime => {
        val deploy = ProtoUtil.termDeploy(term, System.currentTimeMillis())
        for {
          (_, Seq(processedDeploy)) <- newEval(deploy :: Nil, runtime, start)
          r <- if (processedDeploy.status.isFailed)
                immutable.Seq.empty[Datum[ListChannelWithRandom]].pure[F]
              else {
                val returnChannel = Channel(Quote(Par().copy(exprs = Seq(Expr(GString(name))))))
                runtime.space.getData(returnChannel)
              }

          //TODO: Is better error handling needed here?
        } yield r

      })(runtime => runtimeContainer.put(runtime).pure[F])

    for {
      seq <- result
    } yield
      for {
        datum   <- seq
        channel <- datum.a.channels
        par     <- channel.channelInstance.quote
      } yield par
  }

  def replayComputeState(hash: StateHash, terms: Seq[InternalProcessedDeploy])(
      implicit scheduler: Scheduler
  ): F[Either[(Option[Deploy], Failed), StateHash]] =
    Sync[F].bracket(runtimeContainer.take().pure[F])(runtime => replayEval(terms, runtime, hash))(
      runtime => runtimeContainer.put(runtime).pure[F]
    )

  def computeState(hash: StateHash, terms: Seq[Deploy])(
      implicit scheduler: Scheduler
  ): F[(StateHash, Seq[InternalProcessedDeploy])] =
    Sync[F].bracket(runtimeContainer.take().pure[F])(runtime => newEval(terms, runtime, hash))(
      runtime => runtimeContainer.put(runtime).pure[F]
    )

  def storageRepr(hash: StateHash): String = {
    val resetRuntime = getResetRuntime(hash)
    val result       = StoragePrinter.prettyPrint(resetRuntime.space.store)
    runtimeContainer.put(resetRuntime)
    result
  }

  def computeBonds(hash: StateHash): F[Seq[Bond]] =
    shawshankRedemption(hash) { resetRuntime =>
      // TODO: Switch to a read only name
      val bondsChannel = Channel(Quote(Par().copy(exprs = Seq(Expr(GString("proofOfStake"))))))
      for {
        bondsChannelData <- resetRuntime.space.getData(bondsChannel)
        _                = runtimeContainer.put(resetRuntime)
      } yield toBondSeq(bondsChannelData)
    }

  private def shawshankRedemption[A](hash: StateHash)(f: Runtime[F] => F[A]): F[A] =
    Sync[F]
      .delay {
        val runtime   = runtimeContainer.take()
        val blakeHash = Blake2b256Hash.fromByteArray(hash.toByteArray)
        Try(runtime.space.reset(blakeHash)) match {
          case Success(_) => runtime
          case Failure(ex) =>
            runtimeContainer.put(runtime)
            throw ex
        }
      }
      .flatMap(f(_))

  private def getResetRuntime(hash: StateHash) = {
    val runtime   = runtimeContainer.take()
    val blakeHash = Blake2b256Hash.fromByteArray(hash.toByteArray)
    Try(runtime.space.reset(blakeHash)) match {
      case Success(_) => runtime
      case Failure(ex) =>
        runtimeContainer.put(runtime)
        throw ex
    }
  }

  private def toBondSeq(data: Seq[Datum[ListChannelWithRandom]]): Seq[Bond] = {
    assert(data.length == 1, s"Data length ${data.length} for bonds map was not 1.")
    val Datum(as: ListChannelWithRandom, _: Boolean, _: Produce) = data.head
    as.channels.head match {
      case Channel(Quote(p)) =>
        p.exprs.head.getEMapBody.ps.map {
          case (validator: Par, bond: Par) =>
            assert(validator.exprs.length == 1, "Validator in bonds map wasn't a single string.")
            assert(bond.exprs.length == 1, "Stake in bonds map wasn't a single integer.")
            val validatorName = validator.exprs.head.getGString
            val stakeAmount   = Math.toIntExact(bond.exprs.head.getGInt)
            Bond(ByteString.copyFrom(Base16.decode(validatorName)), stakeAmount)
        }.toList
      case Channel(_) => throw new Error("Matched a Channel that did not contain a Quote inside.")
    }
  }

  def getData(hash: ByteString, channel: Channel): F[Seq[Par]] = {
    val resetRuntime = getResetRuntime(hash)
    runtimeContainer.put(resetRuntime)
    for {
      result  <- resetRuntime.space.getData(channel)
      datum   <- result
      channel <- datum.a.channels
      par     <- channel.channelInstance.quote
    } yield par
  }

  def getContinuation(
      hash: ByteString,
      channels: immutable.Seq[Channel]
  ): F[Seq[(Seq[BindPattern], Par)]] = {
    val resetRuntime = getResetRuntime(hash)
    for {
      results <- resetRuntime.space.getWaitingContinuations(channels)
      _       <- Sync[F].delay { runtimeContainer.put(resetRuntime) }
      result  <- results.filter(_.continuation.taggedCont.isParBody)
    } yield (result.patterns, result.continuation.taggedCont.parBody.get.body)
  }

  private def newEval(
      terms: Seq[Deploy],
      runtime: Runtime[F],
      initHash: StateHash
  ): F[(StateHash, Seq[InternalProcessedDeploy])] = {

//    @tailrec <- figure out how to make this tailrec
    def doEval(
        terms: Seq[Deploy],
        hash: Blake2b256Hash,
        acc: Vector[InternalProcessedDeploy]
    ): F[(StateHash, Seq[InternalProcessedDeploy])] =
      terms match {
        case deploy +: rem =>
          runtime.space.reset(hash)
          val availablePhlos             = CostAccount(Integer.MAX_VALUE)
          implicit val costAccountingAlg = CostAccountingAlg.unsafe[F](availablePhlos) //FIXME this needs to come from the deploy params
          for {
            (phlosLeft, errors) <- injAttempt(deploy, runtime.reducer, runtime.errorLog)
            cost <- Sync[F].delay {
                     phlosLeft.copy(cost = availablePhlos.cost.value - phlosLeft.cost)
                   }
            newCheckpoint <- runtime.space.createCheckpoint()
            deployResult <- Sync[F].delay {
                             InternalProcessedDeploy(
                               deploy,
                               cost,
                               newCheckpoint.log,
                               DeployStatus.fromErrors(errors)
                             )
                           }
            result <- if (errors.isEmpty) doEval(rem, newCheckpoint.root, acc :+ deployResult)
                     else doEval(rem, hash, acc :+ deployResult)
          } yield result

        case _ => Sync[F].delay { (ByteString.copyFrom(hash.bytes.toArray), acc) }
      }

    doEval(terms, Blake2b256Hash.fromByteArray(initHash.toByteArray), Vector.empty)
  }

  private def replayEval(
      terms: Seq[InternalProcessedDeploy],
      runtime: Runtime[F],
      initHash: StateHash
  ): F[Either[(Option[Deploy], Failed), StateHash]] = {

    def doReplayEval(
        terms: Seq[InternalProcessedDeploy],
        hash: Blake2b256Hash
    ): F[Either[(Option[Deploy], Failed), StateHash]] =
      terms match {
        case InternalProcessedDeploy(deploy, _, log, status) +: rem =>
          val availablePhlos             = CostAccount(Integer.MAX_VALUE)
          implicit val costAccountingAlg = CostAccountingAlg.unsafe[F](availablePhlos) // FIXME: This needs to come from the deploy params
          runtime.replaySpace.rig(hash, log.toList)
          //TODO: compare replay deploy cost to given deploy cost
          for {
            (phlosLeft, errors) <- injAttempt(deploy, runtime.replayReducer, runtime.errorLog)
            result <- {
              val cost = phlosLeft.copy(cost = availablePhlos.cost.value - phlosLeft.cost)
              DeployStatus.fromErrors(errors) match {
                case int: InternalErrors => Sync[F].delay { Left(Some(deploy) -> int) }
                case replayStatus =>
                  if (status.isFailed != replayStatus.isFailed)
                    Sync[F].delay {
                      Left(Some(deploy) -> ReplayStatusMismatch(replayStatus, status))
                    } else if (errors.nonEmpty)
                    doReplayEval(rem, hash)
                  else {
                    for {
                      result <- runtime.replaySpace.createCheckpoint().map { Try(_) }
                    } yield
                      result match {
                        case Success(newCheckpoint) =>
                          doReplayEval(rem, newCheckpoint.root)
                        case Failure(ex: ReplayException) =>
                          Sync[F].delay { Left(none[Deploy] -> UnusedCommEvent(ex)) }
                      }
                  }
              }
            }
          } yield result

        case _ => Sync[F].delay { Right(ByteString.copyFrom(hash.bytes.toArray)) }
      }

    doReplayEval(terms, Blake2b256Hash.fromByteArray(initHash.toByteArray))
  }

  private def injAttempt(deploy: Deploy, reducer: Reduce[F], errorLog: ErrorLog)(
      implicit costAlg: CostAccountingAlg[F]
  ): F[(PCost, Vector[Throwable])] = {
    implicit val rand: Blake2b512Random = Blake2b512Random(
      DeployData.toByteArray(ProtoUtil.stripDeployData(deploy.raw.get))
    )
    for {
      result <- reducer.inj(deploy.term.get).map(Try(_))
      result2 <- {
        result match {
          case Success(_) =>
            val errors = errorLog.readAndClearErrorVector()
            for {
              costF <- costAlg.get()
            } yield {
              val cost = CostAccount.toProto(costF)
              cost -> errors
            }

          case Failure(ex) =>
            val otherErrors = errorLog.readAndClearErrorVector()
            val errors      = otherErrors :+ ex
            for {
              costF <- costAlg.get()
            } yield {
              val cost = CostAccount.toProto(costF)
              cost -> errors
            }
        }
      }
    } yield result2

  }
}

object RuntimeManager {
  type StateHash = ByteString

  def fromRuntime(active: Runtime[Task]): Task[RuntimeManager[Task]] =
    for {
      _ <- active.space.clear()
      _ <- active.replaySpace.clear()
      hash <- active.space
               .createCheckpoint()
               .map(chck => ByteString.copyFrom(chck.root.bytes.toArray))
      replayHash <- active.replaySpace
                     .createCheckpoint()
                     .map(chck => ByteString.copyFrom(chck.root.bytes.toArray))
      _       <- Task { assert(hash == replayHash) }
      runtime <- Task { new SyncVar[Runtime[Task]]() }
      _       <- Task { runtime.put(active) }
    } yield new RuntimeManager(hash, runtime)
}
