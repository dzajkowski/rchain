package coop.rchain.rspace

import java.nio.ByteBuffer

import cats.Monad
import coop.rchain.catscontrib.Capture
import org.lmdbjava.{Env, Txn}
import cats.implicits._

trait Transactional[F[_], T] {
  def createTxnRead(): F[T]

  def createTxnWrite(): F[T]

  def withTxn[R](txn: F[T])(f: T => F[R]): F[R]
}

object Transactional {
  def apply[F[_], T](implicit ev: Transactional[F, T]): Transactional[F, T] = ev

  type LMDBTransactional[F[_]] = Transactional[F, Txn[ByteBuffer]]
  object LMDBTransactional {
    def apply[F[_]](
        implicit ev: Transactional[F, Txn[ByteBuffer]]): Transactional[F, Txn[ByteBuffer]] = ev
  }

  def lmdbTransactional[F[_]: Capture: Monad](env: Env[ByteBuffer]) =
    new Transactional[F, Txn[ByteBuffer]] {
      def createTxnRead(): F[Txn[ByteBuffer]] =
        Capture[F].capture { env.txnRead }

      def createTxnWrite(): F[Txn[ByteBuffer]] =
        Capture[F].capture { env.txnWrite }

      def withTxnInternal[R](txn: Txn[ByteBuffer])(f: Txn[ByteBuffer] => R): R =
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

      def withTxn[R](t: F[Txn[ByteBuffer]])(f: Txn[ByteBuffer] => F[R]): F[R] =
        t >>= ((_t: Txn[ByteBuffer]) => {
          withTxnInternal(_t) { txn =>
            f(txn)
          }
        })
    }
}
