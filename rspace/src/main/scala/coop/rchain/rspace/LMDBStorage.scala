package coop.rchain.rspace

import java.nio.ByteBuffer

import org.lmdbjava.{Dbi, Env, Txn}
import scodec.Codec
import coop.rchain.shared.ByteVectorOps._
import coop.rchain.shared.AttemptOps._
import scodec.bits.BitVector

trait LMDBStorage {
  private[rspace] def env: Env[ByteBuffer]

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

  /** These methods make an assumption that is true in the current execution,
    * but will likely need to be adjusted if the Store & TrieStore move back to abstract keys:
    * it is good enough to use the raw Blake2b256Hash instead of the scodec Codec[Blake2b256Hash].encode
    *
    * this assumption has two gains:
    * - the methods are less complex
    * - the encoding is faster ()
    *
    * the original code:
    * Codec[Blake2b256Hash].encode(key).get.bytes.toDirectByteBuffer
    *
    */
  implicit class RichDbi(val dbi: Dbi[ByteBuffer]) {
    def put[V](txn: Txn[ByteBuffer], key: Blake2b256Hash, data: V)(
        implicit codecV: Codec[V]): Unit = {
      val keyBuff  = key.bytes.toDirectByteBuffer
      val dataBuff = codecV.encode(data).map(_.bytes.toDirectByteBuffer).get
      if (!dbi.put(txn, keyBuff, dataBuff)) {
        throw new Exception(s"could not persist: $data")
      }
    }

    def delete[V](txn: Txn[ByteBuffer], key: Blake2b256Hash): Unit = {
      val keyBuff = key.bytes.toDirectByteBuffer
      if (!dbi.delete(txn, keyBuff)) {
        throw new Exception(s"could not delete: $key")
      }
    }

    def get[V](txn: Txn[ByteBuffer], key: Blake2b256Hash)(implicit codecV: Codec[V]): Option[V] = {
      val keyBuff = key.bytes.toDirectByteBuffer
      Option(dbi.get(txn, keyBuff))
        .map { bytes =>
          codecV.decode(BitVector(bytes)).map(_.value).get
        }
    }
  }
}
