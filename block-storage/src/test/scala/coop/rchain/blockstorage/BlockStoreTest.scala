package coop.rchain.blockstorage

import cats.mtl.MonadState
import cats.{Applicative, Id, Monad}
import com.google.protobuf.ByteString
import coop.rchain.blockstorage.BlockStore.BlockHash
import coop.rchain.casper.protocol.{BlockMessage, Body, Header, Justification}
import coop.rchain.crypto.hash.Blake2b256
import coop.rchain.metrics.Metrics
import coop.rchain.metrics.Metrics.MetricsNOP
import org.scalactic.anyvals.PosInt
import org.scalatest._
import org.scalatest.prop.GeneratorDrivenPropertyChecks

import scala.language.higherKinds

class BlockStoreTest
    extends FlatSpec
    with Matchers
    with OptionValues
    with GeneratorDrivenPropertyChecks
    with BeforeAndAfterEach {

  implicit override val generatorDrivenConfig: PropertyCheckConfiguration =
    PropertyCheckConfiguration(minSuccessful = PosInt(100))

  override def beforeEach() {}

  override def afterEach() {}

  def bm(v: Long, ts: Long): BlockMessage =
    BlockMessage().withHeader(Header().withVersion(v).withTimestamp(ts))

  def withStore[R](f: BlockStore[Id] => R): R = {
    implicit val applicativeId: Applicative[Id] = new Applicative[Id] {
      override def pure[A](x: A): Id[A] = x

      override def ap[A, B](ff: Id[A => B])(fa: Id[A]): Id[B] = ff.apply(fa)
    }
    implicit val metrics: Metrics[Id] = new MetricsNOP[Id]()
    implicit val state: MonadState[Id, Map[BlockHash, BlockMessage]] =
      new MonadState[Id, Map[BlockHash, BlockMessage]] {

        val monad: Monad[Id] = implicitly[Monad[Id]]

        var map: Map[BlockHash, BlockMessage] = Map.empty

        def get: Id[Map[BlockHash, BlockMessage]] = monad.pure(map)

        def set(s: Map[BlockHash, BlockMessage]): Id[Unit] = ???

        def inspect[A](f: Map[BlockHash, BlockMessage] => A): Id[A] = ???

        def modify(f: Map[BlockHash, BlockMessage] => Map[BlockHash, BlockMessage]): Id[Unit] = {
          map = f(map)
          monad.pure(())
        }
      }
    val store = BlockStore.createMapBased[Id]
    f(store)
  }

  //TODO make generative
  "Block Store" should "return None on get while it's empty" in withStore { store =>
    val key: BlockHash = ByteString.copyFrom("testkey", "utf-8")
    store.get(key) shouldBe None
  }

  //TODO make generative
  "Block Store" should "return Some(message) on get for a published key" in withStore { store =>
    val items = 0 to 100 map { i =>
      val key: BlockHash = ByteString.copyFrom("testkey" + i, "utf-8")
      val msg: BlockMessage = bm(100L + i, 10000L + i)
      (key, msg)
    }
    items.foreach { case (k, v) => store.put(k, v) }
    items.foreach { case (k, v) =>
      store.get(k) shouldBe Some(v)
    }
    //TODO verify that no other values are present
  }
}