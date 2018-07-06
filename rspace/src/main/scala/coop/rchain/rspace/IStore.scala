package coop.rchain.rspace

import java.util.concurrent.atomic.AtomicLong

import coop.rchain.rspace.history.{Branch, ITrieStore}
import coop.rchain.rspace.internal._
import coop.rchain.shared.SyncVarOps

import scala.collection.immutable.Seq
import scala.concurrent.SyncVar

/** The interface for the underlying store
  *
  * @tparam C a type representing a channel
  * @tparam P a type representing a pattern
  * @tparam A a type representing an arbitrary piece of data
  * @tparam K a type representing a continuation
  */
trait IStore[F[_], C, P, A, K] {

  /**
    * The type of transactions
    */
  private[rspace] type T

  private[rspace] type TT // trie transaction

  private[rspace] def withTrieTxn[R](txn: T)(f: TT => R): R

  private[rspace] def hashChannels(channels: Seq[C]): Blake2b256Hash

  private[rspace] def getChannels(txn: T, channelsHash: Blake2b256Hash): Seq[C]

  private[rspace] def putDatum(txn: T, channels: Seq[C], datum: Datum[A]): Unit

  private[rspace] def getData(txn: T, channels: Seq[C]): Seq[Datum[A]]

  private[rspace] def removeDatum(txn: T, channel: Seq[C], index: Int): Unit

  private[rspace] def putWaitingContinuation(txn: T,
                                             channels: Seq[C],
                                             continuation: WaitingContinuation[P, K]): Unit

  private[rspace] def getWaitingContinuation(txn: T,
                                             channels: Seq[C]): Seq[WaitingContinuation[P, K]]

  private[rspace] def removeWaitingContinuation(txn: T, channels: Seq[C], index: Int): Unit

  private[rspace] def getPatterns(txn: T, channels: Seq[C]): Seq[Seq[P]]

  private[rspace] def removeAll(txn: T, channels: Seq[C]): Unit

  private[rspace] def addJoin(txn: T, channel: C, channels: Seq[C]): Unit

  private[rspace] def getJoin(txn: T, channel: C): Seq[Seq[C]]

  private[rspace] def removeJoin(txn: T, channel: C, channels: Seq[C]): Unit

  private[rspace] def joinMap: F[Map[Blake2b256Hash, Seq[Seq[C]]]]

  def toMap: F[Map[Seq[C], Row[P, A, K]]]

  private[rspace] def close(): Unit

  def getStoreCounters: StoreCounters

  val trieStore: ITrieStore[TT, Blake2b256Hash, GNAT[C, P, A, K]]

  val trieBranch: Branch

  private[rspace] val eventsCounter: StoreEventsCounter

  protected val _trieUpdates: SyncVar[Seq[TrieUpdate[C, P, A, K]]] =
    SyncVarOps.create(Seq.empty)

  def trieDelete(key: Blake2b256Hash, gnat: GNAT[C, P, A, K]) = {
    val count   = _trieUpdateCount.getAndIncrement()
    val currLog = _trieUpdates.take()
    _trieUpdates.put(currLog :+ TrieUpdate(count, Delete, key, gnat))
  }

  def trieInsert(key: Blake2b256Hash, gnat: GNAT[C, P, A, K]) = {
    val count   = _trieUpdateCount.getAndIncrement()
    val currLog = _trieUpdates.take()
    _trieUpdates.put(currLog :+ TrieUpdate(count, Insert, key, gnat))
  }

  protected val _trieUpdateCount: AtomicLong = new AtomicLong(0L)

  protected def processTrieUpdate: PartialFunction[TrieUpdate[C, P, A, K], Unit]

  def createCheckpoint(): Blake2b256Hash = {
    val trieUpdates = _trieUpdates.take
    _trieUpdates.put(Seq.empty)
    _trieUpdateCount.set(0L)
    collapse(trieUpdates).foreach(processTrieUpdate)
    trieStore.withTxn(trieStore.createTxnWrite()) { txn => // huh
      trieStore
        .persistAndGetRoot(txn, trieBranch)
        .getOrElse(throw new Exception("Could not get root hash"))
    }
  }

  private[rspace] def collapse(in: Seq[TrieUpdate[C, P, A, K]]): Seq[TrieUpdate[C, P, A, K]] =
    in.groupBy(_.channelsHash)
      .flatMap {
        case (_, value) =>
          value
            .sorted(Ordering.by((tu: TrieUpdate[C, P, A, K]) => tu.count).reverse)
            .headOption match {
            case Some(TrieUpdate(_, Delete, _, _))          => List.empty
            case Some(insert @ TrieUpdate(_, Insert, _, _)) => List(insert)
            case _                                          => value
          }
      }
      .toList

  private[rspace] def bulkInsert(txn: T, gnats: Seq[(Blake2b256Hash, GNAT[C, P, A, K])]): Unit

  private[rspace] def clear(txn: T): Unit

  def isEmpty: F[Boolean]
}

object IStore {
  type AUX[F[_], C, P, A, K, TXN] = IStore[F, C, P, A, K] {
    type T = TXN
  }
}
