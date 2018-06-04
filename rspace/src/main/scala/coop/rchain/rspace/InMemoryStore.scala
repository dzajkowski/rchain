package coop.rchain.rspace

import cats.implicits._
import coop.rchain.rspace.history.{Blake2b256Hash, ITrieStore}
import coop.rchain.rspace.internal._
import coop.rchain.rspace.util.{dropIndex, removeFirst}
import coop.rchain.shared.AttemptOps._
import scodec.Codec
import scodec.bits.BitVector

import scala.collection.immutable.Seq

trait Transaction {
  def commit()
  def abort()
  def close()
}

case class State[C, P, A, K](dbGNATs: Map[Blake2b256Hash, GNAT[C, P, A, K]],
                             dbJoins: Map[C, Seq[Seq[C]]]) {
  def isEmpty: Boolean = dbGNATs.isEmpty && dbJoins.isEmpty
}

object State {
  def empty[C, P, A, K]: State[C, P, A, K] = State[C, P, A, K](Map.empty, Map.empty)
}

class InMemoryStore[C, P, A, K](
    val trieStore: ITrieStore[Unit, Blake2b256Hash, GNAT[C, P, A, K]]
)(implicit sc: Serialize[C], sk: Serialize[K])
    extends IStore[C, P, A, K]
    with ITestableStore[C, P] {

  private implicit val codecC: Codec[C] = sc.toCodec

  val eventsCounter: StoreEventsCounter = new StoreEventsCounter()

  private[this] val stateX: State[C, P, A, K] = State.empty

  private[rspace] type H = Blake2b256Hash

  private[rspace] type T = Transaction with StateAccess

  private[this] type Join      = Seq[Seq[C]]
  private[this] type StateType = State[C, P, A, K]

  private[rspace] def hashChannels(channels: Seq[C]): H =
    Codec[Seq[C]]
      .encode(channels)
      .map((bitVec: BitVector) => Blake2b256Hash.create(bitVec.toByteArray))
      .get

  trait StateAccess {
    def state: StateType = stateX
  }

  private[rspace] def createTxnRead(): T = new Transaction with StateAccess {
    override def commit(): Unit = {}

    override def abort(): Unit = {}

    override def close(): Unit = {}
  }

  private[rspace] def createTxnWrite(): T = new Transaction with StateAccess {
    override def commit(): Unit = {}

    override def abort(): Unit = {}

    override def close(): Unit = {}
  }

  private[rspace] def withTxn[R](txn: T)(f: T => R): R =
    try {
      val ret: R = f(txn)
      txn.commit()
      ret
    } catch {
      case ex: Throwable =>
        txn.abort()
        throw ex
    } finally {
      txn.close()
    }

  private[rspace] def getChannels(txn: T, key: H) =
    txn.state.dbGNATs.get(key).map(_.channels).getOrElse(Seq.empty)

  private[rspace] def getData(txn: T, channels: Seq[C]): Seq[Datum[A]] =
    txn.state.dbGNATs.get(hashChannels(channels)).map(_.data).getOrElse(Seq.empty)

  private[this] def getMutableWaitingContinuation(
      txn: T,
      channels: Seq[C]): Seq[WaitingContinuation[P, K]] =
    txn.state.dbGNATs
      .get(hashChannels(channels))
      .map(_.wks)
      .getOrElse(Seq.empty)

  private[rspace] def getWaitingContinuation(txn: T,
                                             channels: Seq[C]): Seq[WaitingContinuation[P, K]] =
    getMutableWaitingContinuation(txn, channels)
      .map { wk =>
        wk.copy(continuation = InMemoryStore.roundTrip(wk.continuation))
      }

  def getPatterns(txn: T, channels: Seq[C]): Seq[Seq[P]] =
    getMutableWaitingContinuation(txn, channels).map(_.patterns)

  private[rspace] def getJoin(txn: T, channel: C): Join =
    txn.state.dbJoins.getOrElse(channel, Seq.empty)

  private[this] def withGNAT(txn: T, key: H)(
      f: Option[GNAT[C, P, A, K]] => Option[GNAT[C, P, A, K]]): Unit = {
    val state     = txn.state
    val gnatOpt   = state.dbGNATs.get(key)
    val resultOpt = f(gnatOpt)
    handleGNATChange(state, key)(resultOpt)
  }

  private[this] def withJoin(txn: T, jKey: C)(f: Option[Join] => Option[Join]): Unit = {
    val state     = txn.state
    val joinOpt   = state.dbJoins.get(jKey)
    val resultOpt = f(joinOpt)
    handleJoinChange(state, jKey)(resultOpt)
  }

  private[this] def handleGNATChange(
      state: StateType,
      key: H): PartialFunction[Option[GNAT[C, P, A, K]], StateType] = {
    case Some(gnat) if !isOrphaned(gnat) => _dbGNATs += key -> gnat
    case _                               => _dbGNATs -= key
  }

  private[this] def handleJoinChange(state: StateType,
                                     key: C): PartialFunction[Option[Join], StateType] = {
    case Some(join) => _dbJoins += key -> join
    case None       => _dbJoins -= key
  }

  private[rspace] def putDatum(txn: T, channels: Seq[C], datum: Datum[A]): Unit =
    withGNAT(txn, hashChannels(channels)) { gnatOpt =>
      gnatOpt
        .map(gnat => gnat.copy(data = datum +: gnat.data))
        .orElse(GNAT(channels = channels, data = Seq(datum), wks = Seq.empty).some)
    }

  private[rspace] def putWaitingContinuation(txn: T,
                                             channels: Seq[C],
                                             continuation: WaitingContinuation[P, K]): Unit =
    withGNAT(txn, hashChannels(channels)) { gnatOpt =>
      gnatOpt
        .map(gnat => gnat.copy(wks = continuation +: gnat.wks))
        .orElse(GNAT(channels = channels, data = Seq.empty, wks = Seq(continuation)).some)
    }

  private[rspace] def addJoin(txn: T, channel: C, channels: Seq[C]): Unit =
    withJoin(txn, channel) { joinOpt =>
      joinOpt
        .filter(!_.exists(_.equals(channels)))
        .map(channels +: _)
        .orElse(Seq(channels).some)
    }

  private[rspace] def removeDatum(txn: T, channels: Seq[C], index: Int): Unit =
    withGNAT(txn, hashChannels(channels)) { gnatOpt =>
      gnatOpt.map(gnat => gnat.copy(data = dropIndex(gnat.data, index)))
    }

  private[rspace] def removeWaitingContinuation(txn: T, channels: Seq[C], index: Int): Unit =
    withGNAT(txn, hashChannels(channels)) { gnatOpt =>
      gnatOpt.map(gnat => gnat.copy(wks = dropIndex(gnat.wks, index)))
    }

  private[rspace] def removeAll(txn: T, channels: Seq[C]): Unit = {
    withGNAT(txn, hashChannels(channels)) { gnatOpt =>
      gnatOpt.map(_.copy(wks = Seq.empty, channels = Seq.empty))
    }
    for (c <- channels) removeJoin(txn, c, channels)
  }

  private[rspace] def removeJoin(txn: T, channel: C, channels: Seq[C]): Unit =
    withGNAT(txn, hashChannels(channels)) { gnatOpt =>
      if (gnatOpt.isEmpty || gnatOpt.get.wks.isEmpty) {
        withJoin(txn, channel) { joinOpt =>
          joinOpt
            .map(removeFirst(_)(_ == channels))
            .filter(_.nonEmpty)
        }
      }
      gnatOpt
    }

  private[rspace] override def removeAllJoins(txn: T, channel: C): Unit =
    withJoin(txn, channel)(_ => none)

  def close(): Unit = ()

  def clear(): Unit = withTxn(createTxnWrite()) { txn =>
    _dbGNATs = Map.empty
    _dbJoins = Map.empty
    eventsCounter.reset()
  }

  def getStoreCounters: StoreCounters = withTxn(createTxnRead()) { txn =>
    val gnatsSize = txn.state.dbGNATs.foldLeft(0) {
      case (acc, (_, GNAT(chs, data, wks))) => acc + (chs.size + data.size + wks.size)
    }
    eventsCounter.createCounters(0, txn.state.size.toLong)
  }

  def isEmpty: Boolean = withTxn(createTxnRead())(txn => txn.isEmpty)

  def toMap: Map[Seq[C], Row[P, A, K]] = withTxn(createTxnRead()) { txn =>
    _dbGNATs.map {
      case (_, GNAT(cs, data, wks)) => (cs, Row(data, wks))
    }
  }

  private[this] def isOrphaned(gnat: GNAT[C, P, A, K]): Boolean =
    gnat.data.isEmpty && gnat.wks.isEmpty

  def getCheckpoint(): Blake2b256Hash = throw new Exception("unimplemented")
}

object InMemoryStore {

  def roundTrip[K: Serialize](k: K): K =
    Serialize[K].decode(Serialize[K].encode(k)) match {
      case Left(ex)     => throw ex
      case Right(value) => value
    }

  def apply[C, P, A, K <: Serializable]()(implicit sc: Serialize[C],
                                          sk: Serialize[K]): InMemoryStore[C, P, A, K] =
    new InMemoryStore[C, P, A, K](
      trieStore = new DummyTrieStore[Unit, Blake2b256Hash, GNAT[C, P, A, K]])
}
