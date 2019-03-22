package coop.rchain.rspace.nrspace

import java.nio.ByteBuffer

import coop.rchain.rspace.internal._
import coop.rchain.rspace.history.{Leaf, Trie}
import coop.rchain.rspace.{Blake2b256Hash, CloseOps}
import coop.rchain.shared.ByteVectorOps._
import org.lmdbjava.{Dbi, Env, Txn}
import scodec.Codec
import scodec.bits.BitVector

import scala.util.control.NonFatal

class DataStore[K, V](
    val env: Env[ByteBuffer],
    private[this] val _dbData: Dbi[ByteBuffer]
)(implicit codecK: Codec[K], codecV: Codec[V])
    extends CloseOps {

  type Data = Trie[K, V]

  val codecTrie: Codec[Data] = Codec[Data]

  private[rspace] def createTxnRead(): Txn[ByteBuffer] = {
    failIfClosed()
    env.txnRead
  }

  private[rspace] def createTxnWrite(): Txn[ByteBuffer] = {
    failIfClosed()
    env.txnWrite
  }

  @SuppressWarnings(Array("org.wartremover.warts.Throw"))
  // TODO stop throwing exceptions
  private[rspace] def withTxn[R](txn: Txn[ByteBuffer])(f: Txn[ByteBuffer] => R): R =
    try {
      val ret: R = f(txn)
      txn.commit()
      ret
    } catch {
      case NonFatal(ex) =>
        ex.printStackTrace()
        throw ex
    } finally {
      txn.close()
    }

  def get(key: Blake2b256Hash): Option[Data] = {
    val keyBytes = key.bytes.toDirectByteBuffer
    val data = Option(withTxn(createTxnRead()) { txn =>
      _dbData.get(txn, keyBytes)
    })
    data.map(decodeData)
  }

  @SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
  def put(key: Blake2b256Hash, data: Data): Unit = {
    val (keyBytes, dataBytes) = encodeData(key, data)
    withTxn(createTxnWrite()) { txn =>
      _dbData.put(txn, keyBytes, dataBytes)
    }
  }

  @inline def decodeData(data: ByteBuffer): Trie[K, V] =
    codecTrie.decode(BitVector(data)).map(_.value).get

  @inline def encodeData(key: Blake2b256Hash, data: Data): (ByteBuffer, ByteBuffer) =
    (key.bytes.toDirectByteBuffer, codecTrie.encode(data).map(_.bytes.toDirectByteBuffer).get)

  def putBulk(list: List[(Blake2b256Hash, Data)]): Unit = {
    val data = list.map { case (k, d) => encodeData(k, d) }
    withTxn(createTxnWrite()) { txn =>
      data.foreach { case (k, d) => _dbData.put(txn, k, d) }
    }
  }

  def getBulk(list: List[Blake2b256Hash]): List[(Blake2b256Hash, Trie[K, V])] = {
    val data = list.map(k => (k, k.bytes.toDirectByteBuffer))
    withTxn(createTxnRead()) { txn =>
      data.map { case (k, bytes) => (k, _dbData.get(txn, bytes)) }
    }.map { case (key, d) => (key, decodeData(d)) }
  }
}
