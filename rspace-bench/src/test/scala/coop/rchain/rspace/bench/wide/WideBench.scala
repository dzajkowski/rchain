package coop.rchain.rspace.bench.wide

import java.util.concurrent.TimeUnit

import coop.rchain.catscontrib.TaskContrib._
import coop.rchain.crypto.hash.Blake2b512Random
import coop.rchain.rholang.interpreter.Runtime
import coop.rchain.rspace.spaces.FineGrainedRSpace
import coop.rchain.shared.StoreType
import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra.Blackhole

import scala.concurrent.Await
import scala.concurrent.duration.Duration

class WideBench {

  @Benchmark
  @BenchmarkMode(Array(Mode.SingleShotTime))
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  @Fork(value = 1)
  @Threads(1)
  @Warmup(iterations = 2)
  @Measurement(iterations = 5)
  def wideReduceFine(bh: Blackhole, state: FineBenchState): Unit = {
    implicit val scheduler = state.scheduler
    val result             = state.runTask.unsafeRunSync
    bh.consume(processErrors(result))
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.SingleShotTime))
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  @Fork(value = 1)
  @Threads(1)
  @Warmup(iterations = 0)
  @Measurement(iterations = 1)
  def inmemWideReduceFine(bh: Blackhole, state: InMemBenchState): Unit = {
    FineGrainedRSpace.current = FineGrainedRSpace.run.start()
    implicit val scheduler = state.scheduler
    val result             = state.runTask.unsafeRunSync
    bh.consume(processErrors(result))
    FineGrainedRSpace.current.finish()
  }
}

@State(Scope.Benchmark)
class FineBenchState extends WideBenchState {
  override def createRuntime() = Runtime.create(dbDir, mapSize, StoreType.LMDB)
}

@State(Scope.Benchmark)
class InMemBenchState extends WideBenchState {
  override def createRuntime() = Runtime.create(dbDir, mapSize, StoreType.Mixed)
}

abstract class WideBenchState extends WideBenchBaseState {

  implicit def rand: Blake2b512Random = Blake2b512Random(128)

  @Setup(value = Level.Iteration)
  override def doSetup(): Unit = {
    FineGrainedRSpace.current = FineGrainedRSpace.setup.start()
    super.doSetup()
    //make sure we always start from clean rspace
    runtime.replaySpace.clear()
    runtime.space.clear()
    processErrors(Await.result(createTest(setupTerm, runtime.reducer).runAsync, Duration.Inf))
    runTask = createTest(term, runtime.reducer)
    FineGrainedRSpace.current.finish()
  }
}
