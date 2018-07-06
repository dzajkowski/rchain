package coop.rchain.rspace

import java.nio.ByteBuffer
import java.nio.file.{Files, Path}

import cats.{Id}
import coop.rchain.rspace.examples.AddressBookExample._
import coop.rchain.rspace.examples.AddressBookExample.implicits._
import coop.rchain.rspace.history.{initialize, Branch, ITrieStore, LMDBTrieStore}
import coop.rchain.rspace.internal.GNAT
import coop.rchain.rspace.internal.codecGNAT
import coop.rchain.rspace.util._
import coop.rchain.catscontrib._
import coop.rchain.rspace.test.{InMemoryStore, State, Transaction}
import org.lmdbjava.{Env, EnvFlags, Txn}
import org.scalatest.BeforeAndAfterAll
import scodec.Codec

import scala.concurrent.SyncVar

trait StorageExamplesTests extends StorageTestsBase[Channel, Pattern, Entry, EntriesCaptor] {

  "CORE-365: A joined consume on duplicate channels followed by two produces on that channel" should
    "return a continuation and the produced data" in withTestSpace { space =>
    val r1 = space.consume(
      List(Channel("friends"), Channel("friends")),
      List(CityMatch(city = "Crystal Lake"), CityMatch(city = "Crystal Lake")),
      new EntriesCaptor,
      persist = false
    )

    r1 shouldBe None

    val r2 = space.produce(Channel("friends"), bob, persist = false)

    r2 shouldBe None

    val r3 = space.produce(Channel("friends"), bob, persist = false)

    r3 shouldBe defined

    runK(r3)
    getK(r3).results shouldBe List(List(bob, bob))

    space.store.isEmpty shouldBe true
  }

  "CORE-365: Two produces on the same channel followed by a joined consume on duplicates of that channel" should
    "return a continuation and the produced data" in withTestSpace { space =>
    val r1 = space.produce(Channel("friends"), bob, persist = false)

    r1 shouldBe None

    val r2 = space.produce(Channel("friends"), bob, persist = false)

    r2 shouldBe None

    val r3 = space.consume(
      List(Channel("friends"), Channel("friends")),
      List(CityMatch(city = "Crystal Lake"), CityMatch(city = "Crystal Lake")),
      new EntriesCaptor,
      persist = false
    )

    r3 shouldBe defined

    runK(r3)
    getK(r3).results shouldBe List(List(bob, bob))

    space.store.isEmpty shouldBe true
  }

  "CORE-365: A joined consume on duplicate channels given twice followed by three produces" should
    "return a continuation and the produced data" in withTestSpace { space =>
    val r1 = space.consume(
      List(Channel("colleagues"), Channel("friends"), Channel("friends")),
      List(CityMatch(city = "Crystal Lake"),
           CityMatch(city = "Crystal Lake"),
           CityMatch(city = "Crystal Lake")),
      new EntriesCaptor,
      persist = false
    )

    r1 shouldBe None

    val r2 = space.produce(Channel("friends"), bob, persist = false)

    r2 shouldBe None

    val r3 = space.produce(Channel("friends"), bob, persist = false)

    r3 shouldBe None

    val r4 = space.produce(Channel("colleagues"), alice, persist = false)

    r4 shouldBe defined

    runK(r4)
    getK(r4).results shouldBe List(List(alice, bob, bob))

    space.store.isEmpty shouldBe true
  }

  "CORE-365: A joined consume on multiple duplicate channels followed by the requisite produces" should
    "return a continuation and the produced data" in withTestSpace { space =>
    val r1 = space.consume(
      List(
        Channel("family"),
        Channel("family"),
        Channel("family"),
        Channel("family"),
        Channel("colleagues"),
        Channel("colleagues"),
        Channel("colleagues"),
        Channel("friends"),
        Channel("friends")
      ),
      List(
        CityMatch(city = "Herbert"),
        CityMatch(city = "Herbert"),
        CityMatch(city = "Herbert"),
        CityMatch(city = "Herbert"),
        CityMatch(city = "Crystal Lake"),
        CityMatch(city = "Crystal Lake"),
        CityMatch(city = "Crystal Lake"),
        CityMatch(city = "Crystal Lake"),
        CityMatch(city = "Crystal Lake")
      ),
      new EntriesCaptor,
      persist = false
    )

    r1 shouldBe None

    val r2  = space.produce(Channel("friends"), bob, persist = false)
    val r3  = space.produce(Channel("family"), carol, persist = false)
    val r4  = space.produce(Channel("colleagues"), alice, persist = false)
    val r5  = space.produce(Channel("friends"), bob, persist = false)
    val r6  = space.produce(Channel("family"), carol, persist = false)
    val r7  = space.produce(Channel("colleagues"), alice, persist = false)
    val r8  = space.produce(Channel("colleagues"), alice, persist = false)
    val r9  = space.produce(Channel("family"), carol, persist = false)
    val r10 = space.produce(Channel("family"), carol, persist = false)

    r2 shouldBe None
    r3 shouldBe None
    r4 shouldBe None
    r5 shouldBe None
    r6 shouldBe None
    r7 shouldBe None
    r8 shouldBe None
    r9 shouldBe None
    r10 shouldBe defined

    runK(r10)
    getK(r10).results shouldBe List(List(carol, carol, carol, carol, alice, alice, alice, bob, bob))

    space.store.isEmpty shouldBe true
  }

  "CORE-365: Multiple produces on multiple duplicate channels followed by the requisite consume" should
    "return a continuation and the produced data" in withTestSpace { space =>
    val r1 = space.produce(Channel("friends"), bob, persist = false)
    val r2 = space.produce(Channel("family"), carol, persist = false)
    val r3 = space.produce(Channel("colleagues"), alice, persist = false)
    val r4 = space.produce(Channel("friends"), bob, persist = false)
    val r5 = space.produce(Channel("family"), carol, persist = false)
    val r6 = space.produce(Channel("colleagues"), alice, persist = false)
    val r7 = space.produce(Channel("colleagues"), alice, persist = false)
    val r8 = space.produce(Channel("family"), carol, persist = false)
    val r9 = space.produce(Channel("family"), carol, persist = false)

    r1 shouldBe None
    r2 shouldBe None
    r3 shouldBe None
    r4 shouldBe None
    r5 shouldBe None
    r6 shouldBe None
    r7 shouldBe None
    r8 shouldBe None
    r9 shouldBe None

    val r10 = space.consume(
      List(
        Channel("family"),
        Channel("family"),
        Channel("family"),
        Channel("family"),
        Channel("colleagues"),
        Channel("colleagues"),
        Channel("colleagues"),
        Channel("friends"),
        Channel("friends")
      ),
      List(
        CityMatch(city = "Herbert"),
        CityMatch(city = "Herbert"),
        CityMatch(city = "Herbert"),
        CityMatch(city = "Herbert"),
        CityMatch(city = "Crystal Lake"),
        CityMatch(city = "Crystal Lake"),
        CityMatch(city = "Crystal Lake"),
        CityMatch(city = "Crystal Lake"),
        CityMatch(city = "Crystal Lake")
      ),
      new EntriesCaptor,
      persist = false
    )

    r10 shouldBe defined
    runK(r10)
    getK(r10).results shouldBe List(List(carol, carol, carol, carol, alice, alice, alice, bob, bob))

    space.store.isEmpty shouldBe true
  }

  "CORE-365: A joined consume on multiple mixed up duplicate channels followed by the requisite produces" should
    "return a continuation and the produced data" in withTestSpace { space =>
    val r1 = space.consume(
      List(
        Channel("family"),
        Channel("colleagues"),
        Channel("family"),
        Channel("friends"),
        Channel("friends"),
        Channel("family"),
        Channel("colleagues"),
        Channel("colleagues"),
        Channel("family")
      ),
      List(
        CityMatch(city = "Herbert"),
        CityMatch(city = "Crystal Lake"),
        CityMatch(city = "Herbert"),
        CityMatch(city = "Crystal Lake"),
        CityMatch(city = "Crystal Lake"),
        CityMatch(city = "Herbert"),
        CityMatch(city = "Crystal Lake"),
        CityMatch(city = "Crystal Lake"),
        CityMatch(city = "Herbert")
      ),
      new EntriesCaptor,
      persist = false
    )

    r1 shouldBe None

    val r2  = space.produce(Channel("friends"), bob, persist = false)
    val r3  = space.produce(Channel("family"), carol, persist = false)
    val r4  = space.produce(Channel("colleagues"), alice, persist = false)
    val r5  = space.produce(Channel("friends"), bob, persist = false)
    val r6  = space.produce(Channel("family"), carol, persist = false)
    val r7  = space.produce(Channel("colleagues"), alice, persist = false)
    val r8  = space.produce(Channel("colleagues"), alice, persist = false)
    val r9  = space.produce(Channel("family"), carol, persist = false)
    val r10 = space.produce(Channel("family"), carol, persist = false)

    r2 shouldBe None
    r3 shouldBe None
    r4 shouldBe None
    r5 shouldBe None
    r6 shouldBe None
    r7 shouldBe None
    r8 shouldBe None
    r9 shouldBe None
    r10 shouldBe defined

    runK(r10)
    getK(r10).results shouldBe List(List(carol, alice, carol, bob, bob, carol, alice, alice, carol))

    space.store.isEmpty shouldBe true
  }
}

class InMemoryStoreStorageExamplesTestsBase
    extends StorageTestsBase[Channel, Pattern, Entry, EntriesCaptor]
    with BeforeAndAfterAll {
  val dbDir: Path   = Files.createTempDirectory("rchain-storage-test-")
  val mapSize: Long = 1024L * 1024L * 1024L

  type InMemTxn = Transaction[State[Channel, Pattern, Entry, EntriesCaptor]]

  type StateType = State[Channel, Pattern, Entry, EntriesCaptor]

  def inMemTxnX(stateRef: SyncVar[StateType]): Transactional[Id, InMemTxn] =
    new Transactional[Id, InMemTxn] {

      def createTxnRead(): Id[InMemTxn] = new Transaction[StateType] {
        val name: String = "read-" + Thread.currentThread().getId

        private[this] val state = stateRef.get

        override def commit(): Unit = {}

        override def abort(): Unit = {}

        override def close(): Unit = {}

        override def readState[R](f: StateType => R): R = f(state)

        override def writeState[R](f: StateType => (StateType, R)): R =
          throw new RuntimeException("read txn cannot write")
      }

      def createTxnWrite(): Id[InMemTxn] =
        new Transaction[StateType] {
          val name: String = "write-" + Thread.currentThread().getId

          private[this] val initial = stateRef.take
          private[this] var current = initial

          override def commit(): Unit =
            stateRef.put(current)

          override def abort(): Unit = stateRef.put(initial)

          override def close(): Unit = {}

          override def readState[R](f: StateType => R): R = f(current)

          override def writeState[R](f: StateType => (StateType, R)): R = {
            val (newState, result) = f(current)
            current = newState
            result
          }
        }

      def withTxn[R](txn: Id[InMemTxn])(f: InMemTxn => Id[R]): Id[R] =
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
    }

  override def withTestSpace[R](f: T => R): R = {
    val env: Env[ByteBuffer] =
      Env
        .create()
        .setMapSize(mapSize)
        .setMaxDbs(8)
        .setMaxReaders(126)
        .open(dbDir.toFile, List(EnvFlags.MDB_NOTLS): _*)

    implicit val cg: Codec[GNAT[Channel, Pattern, Entry, EntriesCaptor]] = codecGNAT(
      implicits.serializeChannel.toCodec,
      implicits.serializePattern.toCodec,
      implicits.serializeInfo.toCodec,
      implicits.serializeEntriesCaptor.toCodec)

    val branch = Branch("inmem")

    val stateRef = new SyncVar[StateType]
    stateRef.put(State.empty)

    implicit val inMemTxn: Transactional[Id, InMemTxn] = inMemTxnX(stateRef)

    val trieStore
      : ITrieStore[Txn[ByteBuffer], Blake2b256Hash, GNAT[Channel, Pattern, Entry, EntriesCaptor]] =
      LMDBTrieStore.create[Blake2b256Hash, GNAT[Channel, Pattern, Entry, EntriesCaptor]](env)

    val testStore = InMemoryStore.create[Channel, Pattern, Entry, EntriesCaptor](trieStore, branch)
    val testSpace =
      new RSpace[Channel, Pattern, Entry, Entry, EntriesCaptor, InMemTxn](testStore, Branch.MASTER)
    testSpace.storeTransactional.withTxn(testSpace.storeTransactional.createTxnWrite())(
      testStore.clear)
    trieStore.withTxn(trieStore.createTxnWrite())(trieStore.clear)
    initialize(trieStore, branch)
    try {
      f(testSpace)
    } finally {
      testStore.close()
    }
  }

  override def afterAll(): Unit = {
    test.recursivelyDeletePath(dbDir)
    super.afterAll()
  }
}

class InMemoryStoreStorageExamplesTests
    extends InMemoryStoreStorageExamplesTestsBase
    with StorageExamplesTests

class LMDBStoreStorageExamplesTestBase
    extends StorageTestsBase[Channel, Pattern, Entry, EntriesCaptor]
    with BeforeAndAfterAll {

  val dbDir: Path   = Files.createTempDirectory("rchain-storage-test-")
  val mapSize: Long = 1024L * 1024L * 1024L

  def noTls: Boolean = false

  override def withTestSpace[R](f: T => R): R = {
    val env = Context.env(dbDir, mapSize, noTls)
    val context: Context[Id, Channel, Pattern, Entry, EntriesCaptor] =
      Context.create(env, dbDir)
    implicit val transactional: Transactional[Id, Txn[ByteBuffer]] =
      Transactional.lmdbTransactional(env)
    val testStore =
      LMDBStore.create[Id, Channel, Pattern, Entry, EntriesCaptor](context, Branch.MASTER)
    val testSpace =
      new RSpace[Channel, Pattern, Entry, Entry, EntriesCaptor, Txn[ByteBuffer], Txn[ByteBuffer]](
        testStore,
        Branch.MASTER)
    try {
      testSpace.storeTransactional.withTxn(testSpace.storeTransactional.createTxnWrite())(txn =>
        testStore.clear(txn))
      f(testSpace)
    } finally {
      testStore.close()
    }
  }

  override def afterAll(): Unit = {
    test.recursivelyDeletePath(dbDir)
    super.afterAll()
  }
}

class LMDBStoreStorageExamplesTest
    extends LMDBStoreStorageExamplesTestBase
    with StorageExamplesTests
