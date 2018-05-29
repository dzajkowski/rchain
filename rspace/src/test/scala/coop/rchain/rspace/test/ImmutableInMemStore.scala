package coop.rchain.rspace.test

import coop.rchain.rspace.internal._
import coop.rchain.rspace.test.ImmutableInMemStore.RichSyncVar
import coop.rchain.rspace.util.dropIndex
import coop.rchain.rspace.{IStore, ITestableStore, Serialize, StoreSize}
import javax.xml.bind.DatatypeConverter.printHexBinary

import scala.collection.immutable.Seq
import scala.concurrent.SyncVar
import scala.util.control.NonFatal

class ImmutableInMemStore[C, P, A, K <: Serializable] private (
    _keys: Map[String, Seq[C]],
    _waitingContinuations: Map[String, Seq[WaitingContinuation[P, K]]],
    _data: Map[String, Seq[Datum[A]]],
    _joinMap: Map[C, Set[String]],
)(implicit sc: Serialize[C], sk: Serialize[K])
    extends IStore[C, P, A, K]
    with ITestableStore[C, P] {

  case class State(
      keys: Map[String, Seq[C]],
      continuations: Map[String, Seq[WaitingContinuation[P, K]]],
      data: Map[String, Seq[Datum[A]]],
      joins: Map[C, Set[String]]
  ) {}

  object State {
    def apply(
        keys: Map[String, Seq[C]],
        continuations: Map[String, Seq[WaitingContinuation[P, K]]],
        data: Map[String, Seq[Datum[A]]],
        joins: Map[C, Set[String]]
    ): State = {
      // defensive copy of potentially mutable data
      val copy = continuations.map {
        case (k, cs) =>
          k -> cs.map(wk => wk.copy(continuation = ImmutableInMemStore.roundTrip(wk.continuation)))
      }
      new State(keys, copy, data, joins)
    }
  }

  val state: Transaction[State] =
    new LockingTransaction[State](State(_keys, _waitingContinuations, _data, _joinMap))

  private[rspace] type H = String

  private[rspace] type T = Transaction[State]

  private[rspace] def withTxn[R](txn: T)(f: T => R): R = f(txn)

  trait Transaction[DATA] {
    def read: DATA
    def write(f: DATA => DATA): Unit
  }

  object LockingTransaction {
    val DEFAULT_TIMEOUT_MS: Long = 100
  }

  class LockingTransaction[DATA](state: DATA) extends Transaction[DATA] {
    val stateRef: SyncVar[DATA] = new SyncVar[DATA]().init(state)

    def read: DATA = stateRef.get(LockingTransaction.DEFAULT_TIMEOUT_MS).get

    def write(f: DATA => DATA): Unit = {
      val prev = stateRef.take(LockingTransaction.DEFAULT_TIMEOUT_MS)
      try {
        val next = f(prev)
        stateRef.put(next)
      } catch {
        case ex: Throwable =>
          stateRef.put(prev)
          throw ex
      }
    }
  }

  private[this] class ReentrantTransaction[DATA](state: DATA) extends Transaction[DATA] {
    var _state: DATA = state

    def read: DATA = state

    def write(f: DATA => DATA): Unit = {
      val prev = _state
      try {
        _state = f(prev)
      } catch {
        case ex: Throwable =>
          _state = prev
          throw ex
      }
    }
  }

  private[this] def putCs(txn: T, channels: Seq[C]): Unit =
    txn.write(
      state => state.copy(keys = state.keys + (hashChannels(channels) -> channels))
    )

  private[rspace] def getChannels(txn: T, s: H): Seq[C] =
    txn.read.keys.getOrElse(s, Seq.empty[C])

  private[rspace] def putDatum(txn: T, channels: Seq[C], datum: Datum[A]): Unit =
    txn.write {
      case state @ State(keys, waitingContinuations, data, joins) =>
        val key          = hashChannels(channels)
        val reentrantTxn = new ReentrantTransaction(state)
        putCs(reentrantTxn, channels)
        val datums = data.getOrElse(key, Seq.empty[Datum[A]])
        State(reentrantTxn._state.keys,
              waitingContinuations,
              data + (key -> (datum +: datums)),
              joins)
    }

  private[rspace] def putWaitingContinuation(txn: T,
                                             channels: Seq[C],
                                             continuation: WaitingContinuation[P, K]): Unit =
    txn.write {
      case state @ State(keys, waitingContinuations, data, joins) =>
        val key          = hashChannels(channels)
        val reentrantTxn = new ReentrantTransaction(state)
        putCs(reentrantTxn, channels)
        val forKey: Seq[WaitingContinuation[P, K]] =
          waitingContinuations.getOrElse(key, Seq.empty[WaitingContinuation[P, K]])

        State(reentrantTxn._state.keys,
              waitingContinuations + (key -> (continuation +: forKey)),
              data,
              joins)
    }

  private[rspace] def getData(txn: T, channels: Seq[C]): Seq[Datum[A]] =
    txn.read.data.getOrElse(hashChannels(channels), Seq.empty[Datum[A]])

  private[rspace] def getWaitingContinuation(txn: T, curr: Seq[C]): Seq[WaitingContinuation[P, K]] =
    txn.read.continuations
      .getOrElse(hashChannels(curr), Seq.empty[WaitingContinuation[P, K]])
      .map { (wk: WaitingContinuation[P, K]) =>
        wk.copy(continuation = ImmutableInMemStore.roundTrip(wk.continuation)) // bake this into State?
      }

  private[rspace] def removeDatum(txn: T, channel: C, index: Int): Unit =
    removeDatum(txn, Seq(channel), index)

  private[rspace] def removeDatum(txn: T, channels: Seq[C], index: Int): Unit = {
    val key = hashChannels(channels)
    txn.write(
      state =>
        state.copy(
          data = state.data
            .get(key)
            .map(as => state.data + (key -> dropIndex(as, index)))
            .getOrElse(state.data)))
    collectGarbage(txn, key)
  }

  private[rspace] def removeWaitingContinuation(txn: T, channels: Seq[C], index: Int): Unit = {
    val key = hashChannels(channels)
    txn.write(
      state =>
        state.copy(
          continuations = state.continuations
            .get(key)
            .map(x => state.continuations + (key -> dropIndex(x, index)))
            .getOrElse(state.continuations)))
    collectGarbage(txn, key)
  }

  private[rspace] def removeAll(txn: T, channels: Seq[C]): Unit = {
    val key = hashChannels(channels)
    txn.write(
      state =>
        state.copy(continuations = state.continuations + (key -> Seq.empty),
                   data = state.data + (key                   -> Seq.empty)))
    for (c <- channels) removeJoin(txn, c, channels) // needs to be in the same txn
  }

  private[rspace] def addJoin(txn: T, c: C, cs: Seq[C]): Unit =
    txn.write(
      state =>
        state.copy(
          joins = state.joins + (c -> (state.joins.getOrElse(c, Set.empty) + hashChannels(cs)))))

  private[rspace] def getJoin(txn: T, c: C): Seq[Seq[C]] =
    txn.read.joins.getOrElse(c, Set.empty[String]).toList.map(getChannels(txn, _))

  private[rspace] def removeJoin(txn: T, c: C, cs: Seq[C]): Unit = {
    val joinKey = hashChannels(Seq(c))
    txn.write {
      case State(keys, waitingContinuations, data, joins) =>
        val csKey                 = hashChannels(cs)
        val hasContinuationValues = waitingContinuations.get(csKey).forall(_.isEmpty)

        def dropKey =
          (value: Set[String]) =>
            value - csKey match {
              case r if r.isEmpty => joins - c
              case removed        => joins + (c -> removed)
          }

        val result =
          Option(joins).filter(_ => !hasContinuationValues).getOrElse {
            joins
              .get(c)
              .map(dropKey)
              .getOrElse(joins)
          }
        State(keys, waitingContinuations, data, result)
    }
    collectGarbage(txn, joinKey)
  }

  private[rspace] def removeAllJoins(txn: T, c: C): Unit = {
    txn.write(state => state.copy(joins = state.joins - c))
    collectGarbage(txn, hashChannels(Seq(c)))
  }

  def close(): Unit = ()

  def getPatterns(txn: T, channels: Seq[C]): Seq[Seq[P]] =
    txn.read.continuations.getOrElse(hashChannels(channels), Nil).map(_.patterns)

  def clear(): Unit =
    withTxn(createTxnWrite()) { txn =>
      txn.write(
        _ =>
          State(Map.empty[String, Seq[C]],
                Map.empty[String, Seq[WaitingContinuation[P, K]]],
                Map.empty[String, Seq[Datum[A]]],
                Map.empty[C, Set[String]]))
    }

  def getStoreSize: StoreSize =
    withTxn(createTxnRead()) { txn =>
      val state = txn.read
      StoreSize(0,
                (state.keys.size +
                  state.continuations.size +
                  state.data.size +
                  state.joins.size).toLong)
    }

  def isEmpty: Boolean =
    withTxn(createTxnRead()) { txn =>
      val state = txn.read
      state.continuations.isEmpty && state.data.isEmpty && state.keys.isEmpty && state.joins.isEmpty
    }

  def toMap: Map[Seq[C], Row[P, A, K]] =
    withTxn(createTxnRead()) { txn =>
      val state = txn.read
      state.keys
        .map {
          case (hash, cs) =>
            val data = state.data.getOrElse(hash, Seq.empty[Datum[A]])
            val wks =
              state.continuations.getOrElse(hash, Seq.empty[WaitingContinuation[P, K]])
            (cs, Row(data, wks))
        }
    }

  private[rspace] def hashChannels(cs: Seq[C])(implicit sc: Serialize[C]): H =
    printHexBinary(InMemoryStore.hashBytes(cs.flatMap(sc.encode).toArray))

  private[rspace] def createTxnRead(): T = state

  private[rspace] def createTxnWrite(): T = state

  private[rspace] def collectGarbage(txn: T,
                                     channelsHash: H,
                                     dataCollected: Boolean = false,
                                     waitingContinuationsCollected: Boolean = false,
                                     joinsCollected: Boolean = false): Unit =
    collectGarbage(txn, channelsHash)

  private[this] def collectGarbage(txn: T, key: H): Unit = txn.write {
    case State(keys, waitingContinuations, data, joins) =>
      val as         = data.get(key).exists(_.nonEmpty)
      val psks       = waitingContinuations.get(key).exists(_.nonEmpty)
      val cs         = keys.getOrElse(key, Seq.empty[C])
      val joinExists = cs.size == 1 && joins.contains(cs.head)

      val nwc = Some(waitingContinuations)
        .filter(_ => psks)
        .getOrElse(waitingContinuations - key)
      val nk = Some(keys)
        .filter(_ => as || psks || joinExists)
        .getOrElse(keys - key)
      val nd = Some(data)
        .filter(_ => as)
        .getOrElse(data - key)
      State(nk, nwc, nd, joins)
  }
}

object ImmutableInMemStore {
  val DEFAULT_TIMEOUT_MS: Long = 100

  def roundTrip[K: Serialize](k: K): K =
    Serialize[K].decode(Serialize[K].encode(k)) match {
      case Left(ex)     => throw ex
      case Right(value) => value
    }

  def create[C, P, A, K <: Serializable](implicit sc: Serialize[C],
                                         sk: Serialize[K]): ImmutableInMemStore[C, P, A, K] =
    new ImmutableInMemStore[C, P, A, K](
      _keys = Map.empty[String, Seq[C]],
      _waitingContinuations = Map.empty[String, Seq[WaitingContinuation[P, K]]],
      _data = Map.empty[String, Seq[Datum[A]]],
      _joinMap = Map.empty[C, Set[String]]
    )

  implicit class RichSyncVar[R](ref: SyncVar[R]) {

    def update(f: R => R): SyncVar[R] = {
      val prev = ref.take(DEFAULT_TIMEOUT_MS)
      try {
        val next = f(prev)
        ref.put(next)
      } catch {
        // simulate rollback
        case ex: Throwable =>
          ref.put(prev)
          throw ex
      }
      ref
    }

    def init(r: R): SyncVar[R] = {
      ref.put(r)
      ref
    }

    def replace(r: R): SyncVar[R] =
      update(_ => r)
  }

}
