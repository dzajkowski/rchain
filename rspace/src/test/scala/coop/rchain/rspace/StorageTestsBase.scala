package coop.rchain.rspace

import java.nio.ByteBuffer
import java.nio.file.{Files, Path}

import cats.Id
import com.typesafe.scalalogging.Logger
import coop.rchain.rspace.examples.StringExamples._
import coop.rchain.rspace.internal._
import coop.rchain.rspace.examples.StringExamples.implicits._
import coop.rchain.rspace.history.{initialize, Branch, ITrieStore, LMDBTrieStore}
import coop.rchain.rspace.test._
import coop.rchain.catscontrib._
import org.lmdbjava.{Env, EnvFlags, Txn}
import org.scalatest._
import scodec.Codec

import scala.concurrent.SyncVar

trait StorageTestsBase[C, P, A, K] extends FlatSpec with Matchers with OptionValues {

  type T = ISpace[C, P, A, A, K]

  val logger: Logger = Logger(this.getClass.getName.stripSuffix("$"))

  override def withFixture(test: NoArgTest): Outcome = {
    logger.debug(s"Test: ${test.name}")
    super.withFixture(test)
  }

  /** A fixture for creating and running a test with a fresh instance of the test store.
    */
  def withTestSpace[S](f: T => S): S
}

class InMemoryStoreTestsBase
    extends StorageTestsBase[String, Pattern, String, StringsCaptor]
    with BeforeAndAfterAll {

  val dbDir: Path   = Files.createTempDirectory("rchain-storage-test-")
  val mapSize: Long = 1024L * 1024L * 4096L

  type InMemTxn = Transaction[State[String, Pattern, String, StringsCaptor]]

  type StateType = State[String, Pattern, String, StringsCaptor]

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

  override def withTestSpace[S](f: T => S): S = {
    implicit val codecString: Codec[String]   = implicitly[Serialize[String]].toCodec
    implicit val codecP: Codec[Pattern]       = implicitly[Serialize[Pattern]].toCodec
    implicit val codecK: Codec[StringsCaptor] = implicitly[Serialize[StringsCaptor]].toCodec
    val env: Env[ByteBuffer] =
      Env
        .create()
        .setMapSize(mapSize)
        .setMaxDbs(8)
        .setMaxReaders(126)
        .open(dbDir.toFile, List(EnvFlags.MDB_NOTLS): _*)

    val branch = Branch("inmem")

    val stateRef = new SyncVar[StateType]
    stateRef.put(State.empty)

    implicit val inMemTxn: Transactional[Id, InMemTxn] = inMemTxnX(stateRef)

    val trieStore
      : ITrieStore[Txn[ByteBuffer], Blake2b256Hash, GNAT[String, Pattern, String, StringsCaptor]] =
      LMDBTrieStore.create[Blake2b256Hash, GNAT[String, Pattern, String, StringsCaptor]](env)

    val testStore =
      InMemoryStore.create[String, Pattern, String, StringsCaptor](trieStore, branch)
    val testSpace =
      new RSpace[String, Pattern, String, String, StringsCaptor, InMemTxn](testStore, branch)
    testSpace.transactional.withTxn(testSpace.transactional.createTxnWrite())(testStore.clear)
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

class LMDBStoreTestsBase
    extends StorageTestsBase[String, Pattern, String, StringsCaptor]
    with BeforeAndAfterAll {

  val dbDir: Path   = Files.createTempDirectory("rchain-storage-test-")
  val mapSize: Long = 1024L * 1024L * 4096L

  override def withTestSpace[S](f: T => S): S = {
    implicit val codecString: Codec[String]   = implicitly[Serialize[String]].toCodec
    implicit val codecP: Codec[Pattern]       = implicitly[Serialize[Pattern]].toCodec
    implicit val codecK: Codec[StringsCaptor] = implicitly[Serialize[StringsCaptor]].toCodec

    val testBranch = Branch("test")
    val context: Context[String, Pattern, String, StringsCaptor] = Context
      .create[String, Pattern, String, StringsCaptor](dbDir, mapSize, List(EnvFlags.MDB_NOTLS))
    implicit val transactional: Transactional[Id, Txn[ByteBuffer]] =
      Transactional.lmdbTransactional(context.env)
    val testStore =
      LMDBStore.create[Id, String, Pattern, String, StringsCaptor](context, testBranch)
    val testSpace =
      new RSpace[String, Pattern, String, String, StringsCaptor, Txn[ByteBuffer]](testStore,
                                                                                  testBranch)

    testSpace.transactional.withTxn(testSpace.transactional.createTxnWrite()) { txn =>
      testStore.clear(txn)
      testStore.trieStore.clear(txn)
    }
    history.initialize(testStore.trieStore, testBranch)
    try {
      f(testSpace)
    } finally {
      testStore.trieStore.close()
      testStore.close()
    }
  }

  override def afterAll(): Unit =
    recursivelyDeletePath(dbDir)
}
