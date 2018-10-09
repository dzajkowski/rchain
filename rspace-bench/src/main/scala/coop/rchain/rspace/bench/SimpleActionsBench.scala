package coop.rchain.rspace.bench
import java.util.concurrent.TimeUnit

import cats.Id
import coop.rchain.rspace.{State => _, _}
import coop.rchain.rspace.ISpace.IdISpace
import coop.rchain.rspace.examples.AddressBookExample._
import coop.rchain.rspace.examples.AddressBookExample.implicits._
import coop.rchain.rspace.history.Branch
import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra.Blackhole

class SimpleActionsBench {

  import SimpleActionsBench._

  @Benchmark
  @BenchmarkMode(Array(Mode.SingleShotTime, Mode.AverageTime))
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  @Warmup(iterations = 1)
  @Fork(value = 1)
  @Measurement(iterations = 1)
  def singleProduce(bh: Blackhole, state: ProduceInMemBenchState) = {
    val res = state.space.produce(state.produceChannel, bob, persist = true)
    assert(res.right.get.isDefined)
    bh.consume(res)
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.SingleShotTime, Mode.AverageTime))
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  @Warmup(iterations = 1)
  @Fork(value = 1)
  @Measurement(iterations = 1)
  def singleConsume(bh: Blackhole, state: ConsumeInMemBenchState) = {
    val res = state.space.consume(
      List(state.consumeChannel),
      state.matches,
      state.captor,
      persist = true
    )
    assert(res.right.get.isDefined)
    bh.consume(res)
  }
}

object SimpleActionsBench {

  abstract class SimpleActionsBenchState {
    var space: IdISpace[Channel, Pattern, Nothing, Entry, Entry, EntriesCaptor] = null

    val consumeChannel = Channel("consume")
    val produceChannel = Channel("produce")
    val matches        = List(CityMatch(city = "Crystal Lake"))
    val captor         = new EntriesCaptor()

    def initSpace() = {}

    @Setup
    def setup() = {
      val context = Context.createInMemory[Channel, Pattern, Entry, EntriesCaptor]()
      assert(context.trieStore.toMap.isEmpty)
      val testStore = InMemoryStore.create(context.trieStore, Branch.MASTER)
      assert(testStore.toMap.isEmpty)
      space = RSpace.create[Id, Channel, Pattern, Nothing, Entry, Entry, EntriesCaptor](
        testStore,
        Branch.MASTER
      )
    }

    @TearDown
    def tearDown() = {
      space.close()
      ()
    }
  }

  @State(Scope.Thread)
  class ConsumeInMemBenchState extends SimpleActionsBenchState {

    def prepareConsume() = {
      (1 to 1000).foreach { _ =>
        space.produce(consumeChannel, bob, persist = true)

      }
      (1 to 2).foreach { i =>
        space.consume(
          List(consumeChannel),
          matches,
          captor,
          persist = true
        )
      }
    }
    @Setup
    override def setup() = {
      super.setup()
      prepareConsume()
      initSpace
    }
  }

  @State(Scope.Thread)
  class ProduceInMemBenchState extends SimpleActionsBenchState {

    def prepareProduce() = {
      (1 to 1000).foreach { _ =>
        space.consume(
          List(produceChannel),
          matches,
          captor,
          persist = true
        )
      }
      (1 to 2).foreach { i =>
        space.produce(produceChannel, bob, persist = true)
      }
    }
    @Setup
    override def setup() = {
      super.setup()
      prepareProduce()
      initSpace
    }
  }
}
