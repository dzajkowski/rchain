package coop.rchain.rspace.nrspace

import java.lang.{Byte => JByte}

import com.typesafe.scalalogging.Logger
import coop.rchain.rspace.Blake2b256Hash
import coop.rchain.rspace.history.{Branch, DeleteException, InsertException, LookupException}
import scodec.bits.ByteVector
import coop.rchain.rspace.internal._
import cats.Eq
import cats.instances.byte._
import cats.instances.option._
import cats.syntax.eq._
import cats.syntax.traverse._
import cats.instances.vector._
import coop.rchain.catscontrib.seq._
import scodec.Codec

import scala.annotation.tailrec
import scala.collection.immutable

sealed trait Action
final case class DelAction[K](key: K)                        extends Action
final case class InsAction[K](key: K, value: Blake2b256Hash) extends Action

object HistoryTrie {
  import scodec.Codec
  import scodec.Codec._
  import scodec.codecs._
  import scodec.bits.{BitVector, ByteVector}
  import coop.rchain.rspace.internal.codecByteVector

  trait ITrieStore[T, K, V] {

    private[rspace] def createTxnRead(): T

    private[rspace] def createTxnWrite(): T

    private[rspace] def withTxn[R](txn: T)(f: T => R): R

    private[rspace] def getRoot(txn: T, branch: Branch): Option[Blake2b256Hash]

    private[rspace] def persistAndGetRoot(txn: T, branch: Branch): Option[Blake2b256Hash]

    private[rspace] def putRoot(txn: T, branch: Branch, hash: Blake2b256Hash): Unit

    private[rspace] def validateAndPutRoot(txn: T, branch: Branch, hash: Blake2b256Hash): Unit

    private[rspace] def getEmptyRoot(txn: T): Blake2b256Hash

    private[rspace] def putEmptyRoot(txn: T, hash: Blake2b256Hash): Unit

    private[rspace] def put(txn: T, key: Blake2b256Hash, value: Trie[K, V]): Unit

    private[rspace] def get(txn: T, key: Blake2b256Hash): Option[Trie[K, V]]

    private[rspace] def toMap: Map[Blake2b256Hash, Trie[K, V]]

    @SuppressWarnings(Array("org.wartremover.warts.Throw"))
    // TODO stop throwing exceptions
    private[rspace] def getLeaves(txn: T, hash: Blake2b256Hash): Seq[Leaf[K, V]] = {
      @tailrec
      def loop(txn: T, ts: Seq[Trie[K, V]], ls: Seq[Leaf[K, V]]): Seq[Leaf[K, V]] =
        ts match {
          case Seq() =>
            ls
          case tries =>
            val (next, acc) = tries.foldLeft((Seq.empty[Trie[K, V]], ls)) {
              case ((nexts, leaves), Skip(_, pointer)) =>
                val next = get(txn, pointer.hash)
                (nexts ++ next.toSeq, leaves)

              case ((nexts, leaves), Node(pointerBlock)) =>
                val children =
                  pointerBlock.children
                    .map { _.hash }
                    .traverse[Option, Trie[K, V]](hash => get(txn, hash))
                    .getOrElse(throw new LookupException("something went wrong"))
                (nexts ++ children, leaves)
              case ((nexts, leaves), leaf: Leaf[K, V]) =>
                (nexts, leaves :+ leaf)
            }
            loop(txn, next, acc)
        }
      get(txn, hash) match {
        case Some(currentRoot) =>
          loop(txn, Seq(currentRoot), Seq.empty[Leaf[K, V]])
        case None =>
          throw new LookupException(s"could not get node at $hash ")
      }
    }

    private[rspace] def clear(txn: T): Unit

    def close(): Unit
  }

  implicit val codecPointer: Codec[Pointer] =
    discriminated[Pointer]
      .by(uint8)
      .subcaseP(0) {
        case value: LeafPointer => value
      }(Blake2b256Hash.codecBlake2b256Hash.as[LeafPointer])
      .subcaseP(1) {
        case node: NodePointer => node
      }(Blake2b256Hash.codecBlake2b256Hash.as[NodePointer])
      .subcaseP(2) {
        case nothing: EmptyPointer.type => nothing
      }(provide(EmptyPointer))

  implicit val codecNonEmptyPointer: Codec[NonEmptyPointer] =
    discriminated[NonEmptyPointer]
      .by(uint8)
      .subcaseP(0) {
        case value: LeafPointer => value
      }(Blake2b256Hash.codecBlake2b256Hash.as[LeafPointer])
      .subcaseP(1) {
        case node: NodePointer => node
      }(Blake2b256Hash.codecBlake2b256Hash.as[NodePointer])

  class PointerBlock private (val toVector: Vector[Pointer]) {

    require(toVector.length == PointerBlock.length, "A PointerBlock's length must be 256")

    def updated(tuples: List[(Int, Pointer)]): PointerBlock =
      new PointerBlock(tuples.foldLeft(toVector) { (vec, curr) =>
        vec.updated(curr._1, curr._2)
      })

    def updated(tuples: (Int, Pointer)*): PointerBlock =
      updated(tuples.toList)

    def children: Vector[NonEmptyPointer] =
      toVector.collect {
        case p: NonEmptyPointer => p
      }

    def childrenWithIndex: Vector[(NonEmptyPointer, Int)] =
      toVector.zipWithIndex.collect {
        case (p: NonEmptyPointer, i) => (p, i)
      }

    override def toString: String = s"PointerBlock(toVector: ${childrenWithIndex})"

    override def equals(obj: scala.Any): Boolean = obj match {
      case pb: PointerBlock => pb.toVector == toVector
      case _                => false
    }

    override def hashCode(): Int = toVector.hashCode()
  }

  object PointerBlock {

    val length = 256

    def create(): PointerBlock = new PointerBlock(Vector.fill(length)(EmptyPointer))

    def create(first: (Int, Pointer)): PointerBlock =
      PointerBlock.create().updated(List(first))

    def create(first: (Int, Pointer), second: (Int, Pointer)): PointerBlock =
      PointerBlock.create().updated(List(first, second))

    def fromVector(vector: Vector[Pointer]): PointerBlock = new PointerBlock(vector)
    implicit val codecPointerBlock: Codec[PointerBlock] =
      vectorOfN(
        provide(length),
        codecPointer
      ).as[PointerBlock]

    def unapply(arg: PointerBlock): Option[Vector[Pointer]] = Option(arg.toVector)
  }

  sealed trait Pointer
  sealed trait NonEmptyPointer extends Pointer {
    def hash: Blake2b256Hash
  }

  final case class NodePointer(hash: Blake2b256Hash) extends NonEmptyPointer
  final case class LeafPointer(hash: Blake2b256Hash) extends NonEmptyPointer
  case object EmptyPointer                           extends Pointer

  sealed trait Trie[+K, +V]                                          extends Product with Serializable
  final case class Leaf[K, V](key: K, value: V)                      extends Trie[K, V]
  final case class Node(pointerBlock: PointerBlock)                  extends Trie[Nothing, Nothing]
  final case class Skip(affix: ByteVector, pointer: NonEmptyPointer) extends Trie[Nothing, Nothing]

  object Trie {

    def create[K, V](): Trie[K, V] = Node(PointerBlock.create())

    implicit def codecTrie[K, V](implicit codecK: Codec[K], codecV: Codec[V]): Codec[Trie[K, V]] =
      discriminated[Trie[K, V]]
        .by(uint8)
        .subcaseP(0) {
          case (leaf: Leaf[K, V]) => leaf
        }((codecK :: codecV).as[Leaf[K, V]])
        .subcaseP(1) {
          case (node: Node) => node
        }(PointerBlock.codecPointerBlock.as[Node])
        .subcaseP(2) {
          case (skip: Skip) => skip
        }((codecByteVector :: codecNonEmptyPointer).as[Skip])

    def hash[K, V](trie: Trie[K, V])(implicit codecK: Codec[K], codecV: Codec[V]): Blake2b256Hash =
      codecTrie[K, V]
        .encode(trie)
        .map((vector: BitVector) => Blake2b256Hash.create(vector.toByteArray))
        .get
  }

  type Parents[K, V] = Seq[(Int, Trie[K, V])]

  implicit class ParentsOps[K, V](val parents: Parents[K, V]) extends AnyVal {

    def countPathLength: Long =
      parents
        .foldLeft(0L)(
          (acc, el) =>
            el match {
              case (_, s: Skip) => acc + s.affix.size
              case _            => acc + 1L
            }
        )

  }
}

final case class HistoryTrie[K, V](root: Blake2b256Hash)(
    implicit
    codecK: Codec[K],
    codecV: Codec[V]
) {

  import HistoryTrie._
  import HTH._

  private val logger: Logger = Logger[this.type]

  def insert(k: K, hash: Blake2b256Hash): HistoryTrie[K, V] = ???
  def delete(k: K): HistoryTrie[K, V]                       = ???

  def process(actions: List[Action]): HistoryTrie[K, V] =
    ???
  @SuppressWarnings(Array("org.wartremover.warts.Throw"))
  // TODO stop throwing exceptions
  def delete[T](store: ITrieStore[T, K, V], branch: Branch, key: K, value: V): Boolean =
    store.withTxn(store.createTxnWrite()) { (txn: T) =>
      // We take the current root hash, preventing other threads from operating on the Trie
      val currentRootHash: Blake2b256Hash =
        store.getRoot(txn, branch).getOrElse(throw new InsertException("could not get root"))
      // Get the current root node
      store.get(txn, currentRootHash) match {
        case None =>
          throw new LookupException(s"No node at $currentRootHash")
        case Some(currentRoot) =>
          // Serialize and convert the key to a `Seq[Byte]`.  This becomes our "path" down the Trie.
          val encodedKey = codecK.encode(key).map(_.bytes.toSeq).get
          // Using this path, get the parents of the given leaf.
          val (tip, parents) = getParents(store, txn, encodedKey, currentRoot)
          tip match {
            // If the "tip" is a node, a leaf with a given key and value does not exist
            // so we put the current root hash back and return false.
            case Node(_) =>
              logger.debug(s"workingRootHash: $currentRootHash")
              false
            // If the "tip" is equal to a leaf containing the given key and value, commence
            // with the deletion process.
            case leaf @ Leaf(_, _) if leaf == Leaf(key, value) =>
              val (hd, nodesToRehash, newNodes) = deleteLeaf(store, txn, parents)
              val rehashedNodes                 = rehash(hd, nodesToRehash)
              val nodesToInsert                 = newNodes ++ rehashedNodes
              val newRootHash                   = insertTries[T](store, txn, nodesToInsert).get
              store.putRoot(txn, branch, newRootHash)
              logger.debug(s"workingRootHash: $newRootHash")
              true
            // The entry is not in the trie
            case Leaf(_, _) =>
              logger.debug(s"workingRootHash: $currentRootHash")
              false
          }
      }
    }

  @SuppressWarnings(Array("org.wartremover.warts.Throw"))
  // TODO stop throwing exceptions
  def insert[T](store: ITrieStore[T, K, V], branch: Branch, key: K, value: V): Unit =
    store.withTxn(store.createTxnWrite()) { (txn: T) =>
      // Get the current root hash
      val currentRootHash: Blake2b256Hash =
        store.getRoot(txn, branch).getOrElse(throw new InsertException("could not get root"))
      // Get the current root node
      store.get(txn, currentRootHash) match {
        case None =>
          throw new LookupException(s"No node at $currentRootHash")
        case Some(currentRoot) =>
          // Serialize and convert the key to a `Seq[Byte]`.  This becomes our "path" down the Trie.
          val encodedKeyNew        = codecK.encode(key).map(_.bytes.toSeq).get
          val encodedKeyByteVector = ByteVector(encodedKeyNew)
          // Create the new leaf and put it into the store
          val newLeaf     = Leaf(key, value)
          val newLeafHash = Trie.hash(newLeaf)
          store.put(txn, newLeafHash, newLeaf)
          // Using the path we created from the key, get the existing parents of the new leaf.
          val (tip, parents) = getParents(store, txn, encodedKeyNew, currentRoot)
          val maybeNewNodes: Option[Seq[(Blake2b256Hash, Trie[K, V])]] = tip match {
            // If the "tip" is the same as the new leaf, then the given (key, value) pair is
            // already in the Trie, so we put the rootHash back and continue
            case existingLeaf @ Leaf(_, _) if existingLeaf == newLeaf =>
              logger.debug(s"workingRootHash: $currentRootHash")
              None
            // If the "tip" is an existing leaf with a different key than the new leaf, then
            // we are in a situation where the new leaf shares some common prefix with the
            // existing leaf.
            // post skip note: due to the nature of skip nodes, this case is not possible
            // with the current implementation of getParents
            case Leaf(ek, _) if key != ek =>
              throw new InsertException("Trie parents tip should not hold a leaf.")
            // If the "tip" is an existing leaf with the same key as the new leaf, but the
            // existing leaf and new leaf have different values, then we are in the situation
            // where we are "updating" an existing leaf
            case Leaf(ek, ev) if key == ek && value != ev =>
              // Update the pointer block of the immediate parent at the given index
              // to point to the new leaf instead of the existing leaf
              Some(updateLeaf(newLeafHash, parents))
            // If the "tip" is an existing node, then we can add the new leaf's hash to the node's
            // pointer block and rehash.
            case Node(pb) =>
              Some(insertLeafAsNodeChild(encodedKeyByteVector, newLeafHash, parents, pb))
            // If the tip is a skip node -> there is no Node in the Trie for this Leaf.
            // need to:
            // - shorten (a new skip that holds the common path)
            // - remove (no new skip, just the new trie)
            // and rectify the structure
            case Skip(affix, ptr) =>
              Some(insertLeafOntoSkipNode(encodedKeyByteVector, newLeafHash, parents, affix, ptr))
          }
          maybeNewNodes match {
            case Some(nodes) =>
              val newRootHash = insertTries(store, txn, nodes).get
              store.putRoot(txn, branch, newRootHash)
              logger.debug(s"workingRootHash: $newRootHash")
            case None =>
              logger.debug(s"insert did not change the trie")
          }
      }
    }

  // eow method
  private[this] def insertTries[T](
      store: ITrieStore[T, K, V],
      txn: T,
      rehashedNodes: Seq[(Blake2b256Hash, Trie[K, V])]
  ): Option[Blake2b256Hash] =
    rehashedNodes.foldLeft(None: Option[Blake2b256Hash]) {
      case (_, (hash, trie)) =>
        store.put(txn, hash, trie)
        Some(hash)
    }

  @SuppressWarnings(Array("org.wartremover.warts.Throw"))
  // TODO stop throwing exceptions
  private[this] def getParents[T](
      store: ITrieStore[T, K, V],
      txn: T,
      path: Seq[Byte],
      curr: Trie[K, V]
  ): (Trie[K, V], Parents[K, V]) = {

    @tailrec
    def parents(depth: Int, curr: Trie[K, V], acc: Parents[K, V]): (Trie[K, V], Parents[K, V]) =
      curr match {
        case node @ Node(pointerBlock) =>
          val index: Int = JByte.toUnsignedInt(path(depth))
          pointerBlock.toVector(index) match {
            case EmptyPointer =>
              (curr, acc)
            case next: NonEmptyPointer =>
              store.get(txn, next.hash) match {
                case None =>
                  throw new LookupException(s"No node at ${next.hash}")
                case Some(trie) =>
                  parents(depth + 1, trie, (index, node) +: acc)
              }
          }
        case s @ Skip(affix, pointer) =>
          val subPath = ByteVector(path).slice(depth.toLong, depth.toLong + affix.size)
          if (subPath === affix) {
            store.get(txn, pointer.hash) match {
              case None =>
                throw new LookupException(s"No node at ${pointer.hash}")
              case Some(next) =>
                val index: Int = JByte.toUnsignedInt(path(depth))
                parents(affix.size.toInt + depth, next, (index, s) +: acc)
            }
          } else {
            (s, acc)
          }
        case leaf =>
          (leaf, acc)
      }
    parents(0, curr, Seq.empty)
  }

  @SuppressWarnings(Array("org.wartremover.warts.Throw"))
  // TODO stop throwing exceptions
  private[this] def lookup[T](
      txn: T,
      store: ITrieStore[T, K, V],
      branchRootHash: Blake2b256Hash,
      key: K
  ): Option[V] = {
    val path = codecK.encode(key).map(_.bytes.toSeq).get

    @tailrec
    def loop(depth: Int, curr: Trie[K, V]): Option[V] =
      curr match {
        case Skip(affix, pointer) =>
          store.get(txn, pointer.hash) match {
            case Some(next) => loop(depth + affix.length.toInt, next)
            case None       => throw new LookupException(s"No node at ${pointer.hash}")
          }

        case Node(pointerBlock) =>
          val index: Int = JByte.toUnsignedInt(path(depth))
          // We use an explicit match here instead of flatMapping in order to make this function
          // tail-recursive
          pointerBlock.toVector(index) match {
            case EmptyPointer =>
              None
            case pointer: NonEmptyPointer =>
              store.get(txn, pointer.hash) match {
                case Some(next) => loop(depth + 1, next)
                case None       => throw new LookupException(s"No node at ${pointer.hash}")
              }
          }
        case Leaf(lk, lv) if key == lk =>
          Some(lv)
        case Leaf(_, _) =>
          None
      }

    for {
      currentRoot <- store.get(txn, branchRootHash)
      res         <- loop(0, currentRoot)
    } yield res
  }

  def lookup[T](store: ITrieStore[T, K, V], rootHash: Blake2b256Hash, key: K): Option[V] =
    store.withTxn(store.createTxnRead()) { (txn: T) =>
      lookup(txn, store, rootHash, key)
    }

  def lookup[T](store: ITrieStore[T, K, V], branch: Branch, key: K): Option[V] =
    store.withTxn(store.createTxnRead()) { (txn: T) =>
      store.getRoot(txn, branch).flatMap(lookup(txn, store, _, key))
    }

  @SuppressWarnings(Array("org.wartremover.warts.Throw"))
  // TODO stop throwing exceptions
  def lookup[T](
      store: ITrieStore[T, K, V],
      branch: Branch,
      keys: immutable.Seq[K]
  ): Option[immutable.Seq[V]] =
    if (keys.isEmpty) {
      throw new IllegalArgumentException("keys can't be empty")
    } else {
      store.withTxn(store.createTxnRead()) { (txn: T) =>
        keys.traverse[Option, V](
          (k: K) => store.getRoot(txn, branch).flatMap(lookup(txn, store, _, k))
        )
      }
    }

  @tailrec
  @SuppressWarnings(Array("org.wartremover.warts.Throw"))
  // TODO stop throwing exceptions
  private[this] def deleteLeaf[T](
      store: ITrieStore[T, K, V],
      txn: T,
      parents: Parents[K, V]
  ): (Trie[K, V], Parents[K, V], Seq[(Blake2b256Hash, Trie[K, V])]) =
    parents match {
      // If the list parents only contains a single Node, we know we are at the root, and we
      // can update the Vector at the given index to `EmptyPointer`
      case Seq((index, Node(pointerBlock))) =>
        (Node(pointerBlock.updated((index, EmptyPointer))), Seq.empty[(Int, Node)], Seq.empty)
      // A skip pointing at a leaf needs to be collapsed
      case (_, Skip(_, LeafPointer(_))) +: tail =>
        deleteLeaf(store, txn, tail)
      // Otherwise a Node needs to be handled
      case (byte, Node(pointerBlock)) +: tail =>
        // Get the children of the immediate parent
        pointerBlock.childrenWithIndex match {
          // If there are no children, then something is wrong, because one of the children
          // should point down to the thing we are trying to delete.
          case Vector() => throw new DeleteException("PointerBlock has no children")
          // If there are is only one child, then we know that it is the thing we are trying to
          // delete, and we can go ahead and move up the trie.
          // post skip node: a Node with one child should be already collapsed to skip -> leaf
          case Vector(_) =>
            deleteLeaf(store, txn, tail)
          // If there are two children, then we know that one of them points down to the thing
          // we are trying to delete.  We then decide how to handle the other child based on
          // whether or not it is a Node or a Leaf
          case c @ Vector(_, _) =>
            val (otherPtr, otherByte) = c.collect {
              case (child, childByte) if childByte != byte => (child, childByte)
            }.head
            otherPtr match {
              // If the other child is a Node we propagate the structure as it should get collapsed
              case NodePointer(hash) =>
                store.get(txn, hash) match {
                  case Some(Node(_)) =>
                    collapseAndUpdatePointerBlock(otherPtr, ByteVector(otherByte), tail)
                  case Some(Skip(affix, ptr)) =>
                    collapseAndUpdatePointerBlock(ptr, ByteVector(otherByte) ++ affix, tail)
                  case _ =>
                    throw new DeleteException("Given pointer could not be processed in delete.")
                }
              // If the other child is a Leaf, then we must propagate it up the trie.
              case lp: LeafPointer =>
                collapseAndUpdatePointerBlock(lp, ByteVector(otherByte), tail)
            }
          // Otherwise if there are > 2 children, update the parent node's Vector at the given
          // index to `EmptyPointer`.
          case _ => (Node(pointerBlock.updated((byte, EmptyPointer))), tail, Seq.empty)
        }
    }

}

object HTH {
  import HistoryTrie._

  // TODO(ht): make this more efficient
  def commonPrefix[A: Eq](a: Seq[A], b: Seq[A]): Seq[A] =
    a.zip(b).takeWhile { case (l, r) => l === r }.map(_._1)

  def rehash[K, V](trie: Trie[K, V], parents: Parents[K, V])(
      implicit
      codecK: Codec[K],
      codecV: Codec[V]
  ): Seq[(Blake2b256Hash, Trie[K, V])] =
    parents.scanLeft((Trie.hash[K, V](trie), trie)) {
      // node with children, just rehash
      case ((lastHash, _), (offset, Node(pointerBlock))) =>
        val node = Node(pointerBlock.updated((offset, NodePointer(lastHash))))
        (Trie.hash[K, V](node), node)
      //point at node from skip
      case ((lastHash, _: Node), (_, s: Skip)) =>
        val node = s.copy(pointer = NodePointer(lastHash))
        (Trie.hash[K, V](node), node)
    }

  @SuppressWarnings(Array("org.wartremover.warts.Throw"))
  // TODO stop throwing exceptions
  def insertLeafOntoSkipNode[T, K, V](
      encodedKeyByteVector: ByteVector,
      newLeafHash: Blake2b256Hash,
      parents: Parents[K, V],
      affix: ByteVector,
      ptr: NonEmptyPointer
  )(implicit codecK: Codec[K], codecV: Codec[V]) = {
    val pathLength = parents.countPathLength
    val subPath    = encodedKeyByteVector.slice(pathLength, pathLength + affix.size)
    val pathRemaining =
      encodedKeyByteVector.slice(pathLength + affix.size, encodedKeyByteVector.length)
    val sharedPrefix        = commonPrefix(affix.toSeq, subPath.toSeq)
    val sharedSubPathLength = sharedPrefix.length.toLong
    val oldSuffix           = affix.splitAt(sharedSubPathLength)._2
    val newSuffix           = subPath.splitAt(sharedSubPathLength)._2

    val (existingPtr, newPtr, newParents) = (oldSuffix.size, newSuffix.size) match {
      case (0, 0)               => throw new InsertException("Found empty affixes")
      case (ol, nl) if ol != nl => throw new InsertException("Affixes have different lengths")
      case (len, _) if len == 1 =>
        val (newPtr, newParents) =
          setupSkipNode(LeafPointer(newLeafHash), pathRemaining)(codecK, codecV)
        (ptr, newPtr, newParents)
      case (len, _) if len > 1 =>
        val (newSkip, newParents) =
          setupSkipNode(LeafPointer(newLeafHash), newSuffix.drop(1) ++ pathRemaining)(
            codecK,
            codecV
          )
        val (oldSkip, oldParents) =
          setupSkipNode(ptr, oldSuffix.drop(1))(codecK, codecV)
        (oldSkip, newSkip, newParents ++ oldParents)
      case _ => throw new InsertException("Affix corrupt in skip node")
    }

    val oldNodeIndex = JByte.toUnsignedInt(oldSuffix(0))
    val newNodeIndex = JByte.toUnsignedInt(newSuffix(0))
    val newCombinedNode = Node(
      PointerBlock.create((oldNodeIndex, existingPtr), (newNodeIndex, newPtr))
    )

    val (toBeAdded, skips) = if (sharedSubPathLength > 0) {
      val combinedHash = Trie.hash(newCombinedNode)(codecK, codecV)
      (
        Skip(ByteVector(sharedPrefix), NodePointer(combinedHash)),
        Seq((combinedHash, newCombinedNode))
      )
    } else {
      (newCombinedNode, Seq.empty)
    }
    val rehashedNodes = rehash(toBeAdded, parents)
    skips ++ newParents ++ rehashedNodes
  }

  def setupSkipNode[T, K, V](
      ptr: NonEmptyPointer,
      incomingAffix: ByteVector
  )(implicit codecK: Codec[K], codecV: Codec[V]) =
    if (incomingAffix.size > 0) {
      val skip     = Skip(incomingAffix, ptr)
      val skipHash = Trie.hash(skip)(codecK, codecV)
      (NodePointer(skipHash), Seq((skipHash, skip)))
    } else {
      (ptr, Seq.empty)
    }

  def insertLeafAsNodeChild[T, K, V](
      encodedKeyByteVector: ByteVector,
      newLeafHash: Blake2b256Hash,
      parents: Parents[K, V],
      pb: PointerBlock
  )(implicit codecK: Codec[K], codecV: Codec[V]) = {
    val pathLength    = parents.countPathLength
    val newLeafIndex  = JByte.toUnsignedInt(encodedKeyByteVector(pathLength))
    val remainingPath = encodedKeyByteVector.splitAt(pathLength + 1)._2
    val (ptr, newParents) =
      setupSkipNode(LeafPointer(newLeafHash), remainingPath)(codecK, codecV)
    val hd            = Node(pb.updated(List((newLeafIndex, ptr))))
    val rehashedNodes = rehash(hd, parents)
    newParents ++ rehashedNodes
  }

  @SuppressWarnings(Array("org.wartremover.warts.Throw"))
  // TODO stop throwing exceptions
  def updateLeaf[T, K, V](
      newLeafHash: Blake2b256Hash,
      parents: Parents[K, V]
  )(implicit codecK: Codec[K], codecV: Codec[V]) = {
    val (hd, tl) = parents match {
      case (idx, Node(pointerBlock)) +: remaining =>
        (Node(pointerBlock.updated((idx, LeafPointer(newLeafHash)))), remaining)
      case (_, Skip(affix, _)) +: remaining =>
        (Skip(affix, LeafPointer(newLeafHash)), remaining)
      case Seq() =>
        throw new InsertException("A leaf had no parents")
    }
    val rehashedNodes = rehash(hd, tl)
    rehashedNodes
  }

  @SuppressWarnings(Array("org.wartremover.warts.Throw"))
  // TODO stop throwing exceptions
  def collapseAndUpdatePointerBlock[T, K, V](
      ptr: NonEmptyPointer,
      incomingAffix: ByteVector,
      parents: Parents[K, V]
  )(
      implicit codecK: Codec[K],
      codecV: Codec[V]
  ): (Trie[K, V], Parents[K, V], Seq[(Blake2b256Hash, Trie[K, V])]) =
    parents match {
      // If the list parents only contains a single Node, we know we are at the root, and we
      // can update the Vector at the given index to point to the node.
      case Seq((byte, Node(pointerBlock))) =>
        val (newPtr, newParents) = setupSkipNode(ptr, incomingAffix)(codecK, codecV)
        (Node(pointerBlock.updated((byte, newPtr))), Seq.empty[(Int, Node)], newParents)
      // if the node is a Skip it can append the collapsed affix
      case (_, Skip(affix, _)) +: tail =>
        (Skip(affix ++ incomingAffix, ptr), tail, Seq.empty)
      // otherwise it has to be a Node
      case (byte, Node(pointerBlock)) +: tail =>
        // Get the children of the immediate parent
        pointerBlock.children match {
          // If there are no children, then something is wrong, because one of the children
          // should point down to the leaf we are trying to propagate up the trie.
          case Vector() => throw new DeleteException("PointerBlock has no children")
          // If there are is only one child, then we know that it is the thing we are trying to
          // propagate upwards, and we can go ahead and do that.
          // post skip node: this case got swallowed in deleteLeaf
          case Vector(_) =>
            throw new DeleteException(
              "PointerBlock with one child on propagation signifies a malformed trie."
            )
          // Otherwise, if there are > 2 children, we can update the parent node's Vector
          // at the given index to point to the propagated node.
          case _ =>
            val (newPtr, newParents) = setupSkipNode(ptr, incomingAffix)(codecK, codecV)
            (Node(pointerBlock.updated((byte, newPtr))), tail, newParents)
        }
    }

}
