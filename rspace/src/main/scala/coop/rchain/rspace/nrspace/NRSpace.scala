package coop.rchain.rspace.nrspace
import java.nio.ByteBuffer
import java.nio.file.Path

import coop.rchain.rspace._
import coop.rchain.rspace.internal.GNAT
import org.lmdbjava.DbiFlags.MDB_CREATE
import org.lmdbjava.{Dbi, Env, EnvFlags}
import scodec.Codec

import scala.collection.immutable

class NRSpace[F[_], C, P, A, R, K](
    dataStore: DataStore[K, GNAT[C, P, A, K]]
) {}

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

  def create[F[_], C, P, A, R, K](path: Path, mapSize: Long)(
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
    val dataStore                = new DataStore[K, GNAT[C, P, A, K]](envData, _dbData)
    new NRSpace[F, C, P, A, R, K](dataStore)
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
