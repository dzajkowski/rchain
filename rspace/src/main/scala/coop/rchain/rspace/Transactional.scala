package coop.rchain.rspace

import java.nio.ByteBuffer

import cats.{Id, Monad}
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

  def lmdbCombined[F[_]: Capture: Monad](transactional: Transactional[F, Txn[ByteBuffer]])
    : Transactional[F, (Txn[ByteBuffer], Txn[ByteBuffer])] =
    new Transactional[F, (Txn[ByteBuffer], Txn[ByteBuffer])] {
      def createTxnRead(): F[(Txn[ByteBuffer], Txn[ByteBuffer])] =
        transactional.createTxnRead().map(txn => (txn, txn))

      def createTxnWrite(): F[(Txn[ByteBuffer], Txn[ByteBuffer])] =
        transactional.createTxnWrite().map(txn => (txn, txn))

      def withTxn[R](t: F[(Txn[ByteBuffer], Txn[ByteBuffer])])(
          f: ((Txn[ByteBuffer], Txn[ByteBuffer])) => F[R]): F[R] =
        transactional.withTxn(t.map(_._1)) { itxn =>
          f((itxn, itxn))
        }
    }

  def combine[F[_]: Monad, TXN1, TXN2](t1: Transactional[F, TXN1],
                                       t2: Transactional[F, TXN2]): Transactional[F, (TXN1, TXN2)] =
    new Transactional[F, (TXN1, TXN2)] {
      def createTxnRead(): F[(TXN1, TXN2)] =
        for {
          txn1 <- t1.createTxnRead()
          txn2 <- t2.createTxnRead()
        } yield (txn1, txn2)

      def createTxnWrite(): F[(TXN1, TXN2)] =
        for {
          txn1 <- t1.createTxnWrite()
          txn2 <- t2.createTxnWrite()
        } yield (txn1, txn2)

      def withTxn[R](t: F[(TXN1, TXN2)])(f: ((TXN1, TXN2)) => F[R]): F[R] =
        t >>= {
          case (txn1, txn2) =>
            t1.withTxn(txn1.pure[F]) { itxn1 =>
              t2.withTxn(txn2.pure[F]) { itxn2 =>
                f(itxn1, itxn2)
              }
            }
        }
    }
}
