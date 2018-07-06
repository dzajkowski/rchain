package coop.rchain.rspace

import java.nio.ByteBuffer
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.{FileVisitResult, Files, Path, SimpleFileVisitor}

import coop.rchain.catscontrib.Capture
import coop.rchain.rspace.history.{Branch, ITrieStore, Leaf, Node, Skip, Trie}
import coop.rchain.shared.Language.ignore
import scodec.bits.BitVector
import scodec.{Attempt, Codec, DecodeResult}

package object test {

  /**
    * Makes a SimpleFileVisitor to delete files and the directories that contained them
    *
    * [[https://docs.oracle.com/javase/8/docs/api/java/nio/file/FileVisitor.html]]
    */
  private def makeDeleteFileVisitor: SimpleFileVisitor[Path] =
    new SimpleFileVisitor[Path] {
      override def visitFile(p: Path, attrs: BasicFileAttributes): FileVisitResult = {
        Files.delete(p)
        FileVisitResult.CONTINUE
      }
      override def postVisitDirectory(p: Path, e: java.io.IOException): FileVisitResult = {
        Files.delete(p)
        FileVisitResult.CONTINUE
      }
    }

  def recursivelyDeletePath(p: Path): Path =
    Files.walkFileTree(p, makeDeleteFileVisitor)

  /**
    * Converts specified byteBuffer to '-' separated string,
    * convenient during debugging
    */
  private[rspace] def toStr(byteBuffer: ByteBuffer): String = {
    byteBuffer.mark()
    val fetched = new Array[Byte](byteBuffer.remaining())
    ignore { byteBuffer.get(fetched) }
    byteBuffer.reset()
    fetched.toSeq.map(x => x.toString).mkString("-")
  }

  def roundTripCodec[T](t: T)(implicit codec: Codec[T]): Attempt[DecodeResult[T]] =
    codec.encode(t).flatMap((vector: BitVector) => codec.decode(vector))

  def offset(d: Int) = ("   " * d)

  def printTree[F[_]: Capture, T, K, V](
      store: ITrieStore[F, T, K, V],
      branch: Branch = Branch.MASTER)(implicit transactionalEv: Transactional[F, T]): Unit =
    transactionalEv.withTxn(transactionalEv.createTxnRead()) { txn =>
      Capture[F].capture {
        def printBranch(d: Int, t: Trie[K, V]): Unit =
          t match {
            case Leaf(key, value) => println(offset(d), "Leaf", key, value)
            case Node(pointerBlock) =>
              println(offset(d), "node")
              pointerBlock.childrenWithIndex.foreach {
                case (p, i) =>
                  val n = store.get(txn, p.hash).get
                  println(offset(d), i, "#", p.hash)
                  printBranch(d + 1, n)
              }
            case Skip(affix, p) =>
              val n = store.get(txn, p.hash).get
              println(offset(d), "skip", affix, "#", p.hash)
              printBranch(d + 1, n)
          }

        val root = store.getRoot(txn, branch)
        println("---------------------")
        println("root#", root)
        val rootNode = store.get(txn, root.get).get
        printBranch(0, rootNode)
        println("---------------------")
      }
    }

}
