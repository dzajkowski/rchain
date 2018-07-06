package coop.rchain.rspace

import java.nio.ByteBuffer
import java.nio.file.Path

import coop.rchain.catscontrib.Capture
import coop.rchain.rspace.Transactional.LMDBTransactional
import coop.rchain.rspace.history.{ITrieStore, LMDBTrieStore}
import coop.rchain.rspace.internal.GNAT
import org.lmdbjava.{Env, EnvFlags, Txn}
import scodec.Codec

class Context[F[_]: Capture: LMDBTransactional, C, P, A, K] private (
    val env: Env[ByteBuffer],
    val path: Path,
    val trieStore: ITrieStore[F, Txn[ByteBuffer], Blake2b256Hash, GNAT[C, P, A, K]]
) {

  def close(): Unit = {
    trieStore.close()
    env.close()
  }
}

object Context {

  def env(path: Path, mapSize: Long, noTls: Boolean): Env[ByteBuffer] =
    env(path,
        mapSize,
        if (noTls)
          List(EnvFlags.MDB_NOTLS)
        else
          List.empty[EnvFlags])

  def env(path: Path,
          mapSize: Long,
          flags: List[EnvFlags] = List(EnvFlags.MDB_NOTLS)): Env[ByteBuffer] =
    Env
      .create()
      .setMapSize(mapSize)
      .setMaxDbs(8)
      .setMaxReaders(126)
      .open(path.toFile, flags: _*)

  def create[F[_]: Capture: LMDBTransactional, C, P, A, K](env: Env[ByteBuffer], path: Path)(
      implicit
      sc: Serialize[C],
      sp: Serialize[P],
      sa: Serialize[A],
      sk: Serialize[K]): Context[F, C, P, A, K] = {

    implicit val codecC: Codec[C] = sc.toCodec
    implicit val codecP: Codec[P] = sp.toCodec
    implicit val codecA: Codec[A] = sa.toCodec
    implicit val codecK: Codec[K] = sk.toCodec

    val trieStore = LMDBTrieStore.create[F, Blake2b256Hash, GNAT[C, P, A, K]](env)

    new Context(env, path, trieStore)
  }
}
