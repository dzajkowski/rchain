package coop.rchain.rspace.nrspace
import coop.rchain.rspace.Blake2b256Hash
import scodec.Codec
import scodec.codecs._
import scodec.bits.{BitVector, ByteVector}
import coop.rchain.rspace.internal.codecByteVector
import coop.rchain.shared.AttemptOps._
import TrieStoreNext._

import scala.annotation.tailrec
import scala.collection.concurrent.TrieMap

//case class HT[K](root: Blake2b256Hash)(implicit codecK: Codec[K]) {
//  // iow state
//  var htx: HistoryTrieX[K] = HistoryTrieX[K](root, trieStore = HTSI(), pointerBlockStore = HPBI())
//
//  def process(actions: List[Action]): Blake2b256Hash = ???
//
//  def get(key: K): Option[Blake2b256Hash] = {
//    val v = htx.getKey(key)
//    v match {
//      case EmptyTrie => None
//      case Leaf(hash) => Some(hash)
//      case _ => throw new RuntimeException("something is royally borked")
//    }
//  }
//}

trait HT {
  def get(k: Blake2b256Hash): Trie
  def put(blake2b256Hash: Blake2b256Hash, t: Trie): Unit
}

trait PBS {
  def get(h: Blake2b256Hash): Option[PointerBlock]
}

//case class HTX[K](ha: HT, pbs: PBS, root: Blake2b256Hash)(
//    implicit codecK: Codec[K],
//    ordering: Ordering[List[Byte]]
//) {
//  def a() = ???
//
//  def process(actions: List[Action[K]]): Blake2b256Hash =
//    ???
//
//  def isPrefix(affix: ByteVector, cp: List[Byte]): Boolean =
//    cp.startsWith(affix.toSeq)
//
//  type TriePath = Vector[Trie]
//
//  private def traverseTrie(path: List[Byte]): (Trie, TriePath) = ???
//
//}

//case class HistoryTrieX[K](root: Blake2b256Hash, fetchedPaths: TrieMap[K, TriePath] = TrieMap.empty, trieStore: HTS, pointerBlockStore: HPB)(implicit val codecK: Codec[K]) {
//
//  def process(actions: List[Action]) = ???
//
//  def isPrefix(affix: ByteVector, cp: List[Byte]): Boolean = {
//    cp.startsWith(affix.toSeq)
//  }
//
//  private def traverseTrie(key: K): (Trie, TriePath) = {
//    // list might be bad here...
//    @tailrec
//    def traverse(t: Trie, cp: List[Byte], path: TriePath): (Trie, TriePath) = {
//      (t, cp) match {
//        case (EmptyTrie, _) => (EmptyTrie, path)
//        case (l:Leaf, Nil) => (t, path :+ PathNode(l))
//        case (s@Skip(affix, p), _) if isPrefix(affix, cp) => traverse(trieStore.get(p), cp.drop(affix.size.toInt), path :+ PathNode(s))
//        case (Node(ptr), h :: tail) =>
//          pointerBlockStore.get(ptr) match {
//            case Some(pb) => traverse(pb(h), tail, path :+ PartialPointerNode(h, pb(h)))
//            case None => throw new RuntimeException("malformed trie")
//          }
//        case _ => throw new RuntimeException("malformed trie")
//      }
//    }
//    val path = codecK.encode(key).get.bytes.toSeq.toList
//    traverse(trieStore.get(root), path, Vector.empty[TriePathNode])
//  }
//
//  def getKey(k: K): Trie = {
//    val (t, p) = traverseTrie(k)
//    t match { //wunderbar...
//      case _ : NonEmptyTrie => fetchedPaths.put(k, p)
//    }
//    t
//  }
//}

object TrieStoreNext {

  trait HTS {
    def get(hash: Blake2b256Hash): Trie
    def put(hash: Blake2b256Hash, value: Trie): Unit
  }

  final case class HTSI() extends HTS {
    override def get(hash: LeafPointer): Trie              = ???
    override def put(hash: LeafPointer, value: Trie): Unit = ???
  }

  trait HPB {
    def get(hash: Blake2b256Hash): Option[PointerBlock]
    def put(hash: Blake2b256Hash, value: PointerBlock): Unit
  }

  final case class HPBI() extends HPB {
    override def get(hash: LeafPointer): Option[PointerBlock]      = ???
    override def put(hash: LeafPointer, value: PointerBlock): Unit = ???
  }

  type PointerBlockPointer = Blake2b256Hash
  type TriePointer         = Blake2b256Hash
  type LeafPointer         = Blake2b256Hash

  sealed trait Trie
  sealed trait NonEmptyTrie                                   extends Trie
  case object EmptyTrie                                       extends Trie
  final case class Skip(affix: ByteVector, hash: TriePointer) extends NonEmptyTrie
  final case class Node(hash: PointerBlockPointer)            extends NonEmptyTrie
  final case class Leaf(hash: LeafPointer)                    extends NonEmptyTrie

  final case class PointerBlock private (toVector: Vector[Trie]) {
    def updated(tuples: List[(Int, Trie)]): PointerBlock =
      new PointerBlock(tuples.foldLeft(toVector) { (vec, curr) =>
        vec.updated(curr._1, curr._2)
      })
  }

  //is this good enough?
  sealed trait TriePathNode
  final case class PathNode(t: NonEmptyTrie)                    extends TriePathNode
  final case class PartialPointerNode(i: Byte, t: NonEmptyTrie) extends TriePathNode

  object Trie {
    def hash(trie: Trie)(implicit codecTrie: Codec[Trie]): Blake2b256Hash =
      codecTrie
        .encode(trie)
        .map((vector: BitVector) => Blake2b256Hash.create(vector.toByteArray))
        .get
  }

  object PointerBlock {
    val length = 256

    def create(): PointerBlock = new PointerBlock(Vector.fill(length)(EmptyTrie))

    def create(first: (Int, Trie)): PointerBlock =
      PointerBlock.create().updated(List(first))

    def fromVector(vector: Vector[Trie]): PointerBlock = new PointerBlock(vector)
    implicit val codecPointerBlock: Codec[PointerBlock] =
      vectorOfN(
        provide(length),
        codecTrie
      ).as[PointerBlock]

    def unapply(arg: PointerBlock): Option[Vector[Trie]] = Option(arg.toVector)
  }

  private val codecNode  = Blake2b256Hash.codecBlake2b256Hash
  private val codecLeaf  = Blake2b256Hash.codecBlake2b256Hash
  private val codecSkip  = codecByteVector :: Blake2b256Hash.codecBlake2b256Hash
  private val codecEmpty = provide(EmptyTrie)

  implicit def codecTrie: Codec[Trie] =
    discriminated[Trie]
      .by(uint2)
      .subcaseP(0) {
        case n: Node => n
      }(codecNode.as[Node])
      .subcaseP(1) {
        case s: Skip => s
      }(codecSkip.as[Skip])
      .subcaseP(2) {
        case emptyTrie: EmptyTrie.type => emptyTrie
      }(codecEmpty)
      .subcaseP(3) {
        case l: Leaf => l
      }(codecLeaf.as[Leaf])
}

object A {
  def main(args: Array[String]): Unit = {
    import coop.rchain.rspace.nrspace.TrieStoreNext._
    val empty = EmptyTrie
    val oneNode = Node(
      Blake2b256Hash.fromHex("ff3c5e70a028b7956791a6b3d8db00000f469e0088db22dd3afbc86997fe86a0")
    )
    val skip = Skip(
      ByteVector("123".getBytes),
      Blake2b256Hash.fromHex("ff3c5e70a028b7956791a6b3d8db00000f469e0088db22dd3afbc86997fe86a0")
    )
    val leaf = Leaf(
      Blake2b256Hash.fromHex("ff3c5e70a028b7956791a6b3d8db00000f469e0088db22dd3afbc86997fe86a0")
    )
    val pb  = PointerBlock.create()
    val pb2 = PointerBlock.create((7, skip))
    val pb3 = PointerBlock.fromVector(Vector.fill(256)(skip))

    println(codecTrie.encode(empty))
    println(codecTrie.encode(oneNode))
    println(codecTrie.encode(skip))
    println(codecTrie.encode(leaf))
    println(PointerBlock.codecPointerBlock.encode(pb))
    println(PointerBlock.codecPointerBlock.encode(pb2))
    println(PointerBlock.codecPointerBlock.encode(pb3))

    println(Trie.hash(empty))
    println(Trie.hash(oneNode))
    println(Trie.hash(skip))
    println(Trie.hash(leaf))
  }
}
