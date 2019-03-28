package coop.rchain.rspace.nrspace
import java.nio.ByteBuffer
import java.nio.file.Path

import cats.{Applicative, ApplicativeError}
import cats.implicits._
import coop.rchain.rspace._
import coop.rchain.rspace.history.{Leaf, Trie}
import coop.rchain.rspace.internal.{Delete, GNAT, Insert, Operation}
import coop.rchain.rspace.trace.Log
import monix.execution.atomic.AtomicAny
import org.lmdbjava.DbiFlags.MDB_CREATE
import org.lmdbjava.{Dbi, Env, EnvFlags}
import scodec.Codec

import scala.collection.concurrent.TrieMap
import scala.collection.immutable.Seq

class NRSpace[F[_]: Applicative, C, P, A, R, K] private[rspace] (
    dataStore: LeafStore[K, GNAT[C, P, A, K]],
    trieStore: TrieStore[C, P, A, K]
)(implicit codecK: Codec[K], codecV: Codec[GNAT[C, P, A, K]]) {

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  var currentHotStore: HotStore[C, P, A, K] = _

  type Data = GNAT[C, P, A, K]

  def produce(): F[Unit] = ???
  def consume(): F[Unit] = ???

  def createCheckpoint(): F[Checkpoint] = {
    val (updates, log) = currentHotStore.checkpoint
    val (nr, data): (Blake2b256Hash, List[(Blake2b256Hash, Trie[K, Data])]) =
      trieStore.process(currentHotStore.trie, updates)
    dataStore.putBulk(data)
    Checkpoint(nr, log).pure[F]
  }

  @SuppressWarnings(Array("org.wartremover.warts.Throw"))
  def reset(root: Blake2b256Hash): F[Unit] = {
    trieStore.getTrie(root) match {
      case None     => throw new RuntimeException(s"no history trie at root $root")
      case Some(ht) => currentHotStore = HotStore.of[C, P, A, K](ht)
    }

    ().pure[F]
  }
}

final case class HotStore[C, P, A, K](trie: HistoryTrie[K, GNAT[C, P, A, K]]) {
  protected[this] val eventLog: AtomicAny[Log] =
    AtomicAny[Log](Seq.empty)

  private val _trieUpdates: AtomicAny[(Long, List[TrieUpdate[C, P, A, K]])] =
    AtomicAny[(Long, List[TrieUpdate[C, P, A, K]])]((0L, Nil))

  def checkpoint: (List[TrieUpdate[C, P, A, K]], Log) =
    (collapse(_trieUpdates.get()._2), eventLog.get())

  private[rspace] def collapse(in: List[TrieUpdate[C, P, A, K]]): List[TrieUpdate[C, P, A, K]] =
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
}

object HotStore {
  def of[C, P, A, K](trie: HistoryTrie[K, GNAT[C, P, A, K]]): HotStore[C, P, A, K] =
    HotStore[C, P, A, K](trie)
}

final case class TrieUpdate[C, P, A, K](
    count: Long,
    operation: Operation,
    channelsHash: K,
    gnat: GNAT[C, P, A, K]
)

class TrieStore[C, P, A, K] {
  type Data = GNAT[C, P, A, K]

  private val tries: TrieMap[Blake2b256Hash, HistoryTrie[K, Data]] = TrieMap.empty

  def getTrie(root: Blake2b256Hash): Option[HistoryTrie[K, Data]] = tries.get(root)

  def process(
      trie: HistoryTrie[K, Data],
      updates: List[TrieUpdate[C, P, A, K]]
  )(
      implicit codecK: Codec[K],
      codecV: Codec[Data]
  ): (Blake2b256Hash, List[(Blake2b256Hash, Trie[K, Data])]) = {
    val data = updates.map {
      case TrieUpdate(_, Delete, channelsHash, _) =>
        (DeleteAction(channelsHash), None)
      case TrieUpdate(_, Insert, channelsHash, gnat) =>
        val leaf: Leaf[K, Data] = Leaf(channelsHash, gnat)
        val hash                = Trie.hash(leaf)
        (InsAction(channelsHash, hash), Some((hash, leaf)))
    }
    val (actions, d) = data.unzip
    val nht          = trie.process(actions)
    (nht.root, d.flatten)
  }
}

object NRSpace {
  def env(
      path: Path,
      mapSize: Long,
      flags: List[EnvFlags] = List(EnvFlags.MDB_NOTLS)
  ): Env[ByteBuffer] =
    Env
      .create()
      .setMapSize(mapSize)
      .setMaxDbs(8)
      .setMaxReaders(2048)
      .open(path.toFile, flags: _*)

  def create[F[_]: Applicative, C, P, A, R, K](path: Path, mapSize: Long)(
      implicit
      sc: Serialize[C],
      sp: Serialize[P],
      sa: Serialize[A],
      sk: Serialize[K]
  ) = {
    implicit val codecC: Codec[C] = sc.toCodec
    implicit val codecP: Codec[P] = sp.toCodec
    implicit val codecA: Codec[A] = sa.toCodec
    implicit val codecK: Codec[K] = sk.toCodec
    implicit val codecGNAT: Codec[GNAT[C, P, A, K]] =
      internal.codecGNAT(codecC, codecP, codecA, codecK)

    val envData                  = env(path, mapSize)
    val _dbData: Dbi[ByteBuffer] = envData.openDbi("Trie", MDB_CREATE)
    val dataStore                = new LeafStore[K, GNAT[C, P, A, K]](envData, _dbData)
    val trieStore                = new TrieStore[C, P, A, K]()
    new NRSpace[F, C, P, A, R, K](dataStore, trieStore)
  }
}

//extends ISpace[F, C, P, A, R, K] {
//
//  override def consume(channels: immutable.Seq[C],
//                       patterns: immutable.Seq[P],
//                       continuation: K,
//                       persist: Boolean,
//                       sequenceNumber: Int)
//                      (implicit m: Match[F, P, A, R]): F[Option[(ContResult[C, P, K], immutable.Seq[Result[R]])]] = ???
//
//  override def install(channels:  immutable.Seq[C], patterns:  immutable.Seq[P], continuation:  K)(implicit m:  Match[F, P, A, R]): F[Option[(K, immutable.Seq[R])]] = ???
//
//  override def produce(channel:  C, data:  A, persist:  Boolean, sequenceNumber:  Int)(implicit m:  Match[F, P, A, R]): F[Option[(ContResult[C, P, K], immutable.Seq[Result[R]])]] = ???
//
//  override def createCheckpoint(): F[Checkpoint] = ???
//
//  override def reset(root:  Blake2b256Hash): F[Unit] = ???
//
//  override def retrieve(root:  Blake2b256Hash, channelsHash:  Blake2b256Hash): F[Option[internal.GNAT[C, P, A, K]]] = ???
//  override def getData(channel:  C): F[immutable.Seq[internal.Datum[A]]] = ???
//  override def getWaitingContinuations(channels:  immutable.Seq[C]): F[immutable.Seq[internal.WaitingContinuation[P, K]]] = ???
//
//  override def clear(): F[Unit] = ???
//  override def close(): F[Unit] = ???
//
//  override protected  def isDirty(root:  Blake2b256Hash): F[Boolean] = ???
//  override val store: IStore[F, C, P, A, K] = _
//}
