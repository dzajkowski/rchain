package coop.rchain.rspace

import java.nio.ByteBuffer

import org.lmdbjava.{Dbi, Env, Txn}

trait LMDBStorage {
  def env: Env[ByteBuffer]

  private[rspace] def createTxnRead(): Txn[ByteBuffer] = env.txnRead

  private[rspace] def createTxnWrite(): Txn[ByteBuffer] = env.txnWrite

  private[rspace] def withTxn[R](txn: Txn[ByteBuffer])(f: Txn[ByteBuffer] => R): R =
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

  implicit class RichDbi[T](val dbi: Dbi[T]) {
    def
  }
}
