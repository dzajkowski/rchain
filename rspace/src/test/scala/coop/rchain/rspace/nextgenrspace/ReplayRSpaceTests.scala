package coop.rchain.rspace.nextgenrspace

import java.nio.file.{Files, Path, Paths}

import cats.Functor
import coop.rchain.catscontrib.TaskContrib._
import coop.rchain.catscontrib.ski._
import cats.effect._
import cats.implicits._
import com.typesafe.scalalogging.Logger
import coop.rchain.metrics.Metrics
import coop.rchain.rspace._
import coop.rchain.rspace.examples.StringExamples._
import coop.rchain.rspace.examples.StringExamples.implicits._
import coop.rchain.rspace.history._
import coop.rchain.rspace.internal._
import coop.rchain.rspace.nextgenrspace.history.{
  HistoryRepository,
  HistoryRepositoryInstances,
  LMDBRSpaceStorageConfig,
  StoreConfig
}
import coop.rchain.rspace.trace.Consume
import coop.rchain.rspace.test._
import coop.rchain.shared.Cell
import coop.rchain.shared.PathOps._
import coop.rchain.shared.Log
import monix.eval.Task
import monix.execution.Scheduler
import monix.execution.atomic.AtomicAny
import org.scalacheck._
import org.scalacheck.Arbitrary._
import org.scalatest._
import org.scalatest.prop._

import scala.util.{Random, Right}
import scala.util.Random.shuffle
import scodec.Codec

import org.lmdbjava.EnvFlags

object SchedulerPools {
  implicit val global = Scheduler.fixedPool("GlobalPool", 20)
  val rspacePool      = Scheduler.fixedPool("RSpacePool", 5)
}

//noinspection ZeroIndexToHead,NameBooleanParameters
trait ReplayRSpaceTests extends ReplayRSpaceTestsBase[String, Pattern, String, String] {

  import SchedulerPools.global

  implicit val log: Log[Task]      = new Log.NOPLog[Task]
  val arbitraryRangeSize: Gen[Int] = Gen.chooseNum[Int](1, 10)
  val arbitraryRangesSize: Gen[(Int, Int)] = for {
    m <- Gen.chooseNum[Int](1, 10)
    n <- Gen.chooseNum[Int](1, m)
  } yield (n, m)

  def consumeMany[C, P, A, R, K](
      space: ISpace[Task, C, P, A, R, K],
      range: Seq[Int],
      channelsCreator: Int => List[C],
      patterns: List[P],
      continuationCreator: Int => K,
      persist: Boolean
  )(
      implicit matcher: Match[Task, P, A, R]
  ): Task[List[Option[(ContResult[C, P, K], Seq[Result[R]])]]] =
    shuffle(range).toList.parTraverse { i: Int =>
      logger.debug("Started consume {}", i)
      space
        .consume(channelsCreator(i), patterns, continuationCreator(i), persist)
        .map { r =>
          logger.debug("Finished consume {}", i)
          r
        }
    }

  def produceMany[C, P, A, R, K](
      space: ISpace[Task, C, P, A, R, K],
      range: Seq[Int],
      channelCreator: Int => C,
      datumCreator: Int => A,
      persist: Boolean
  )(
      implicit matcher: Match[Task, P, A, R]
  ): Task[List[Option[(ContResult[C, P, K], Seq[Result[R]])]]] =
    shuffle(range).toList.parTraverse { i: Int =>
      logger.debug("Started produce {}", i)
      space.produce(channelCreator(i), datumCreator(i), persist).map { r =>
        logger.debug("Finished produce {}", i)
        r
      }
    }

  "reset to a checkpoint from a different branch" should "work" in fixture {
    (store, replayStore, space, replaySpace) =>
      for {
        root0 <- replaySpace.createCheckpoint().map(_.root)
        _     = replayStore.get().isEmpty.map(_ shouldBe true)

        _     <- space.produce("ch1", "datum1", false)
        root1 <- space.createCheckpoint().map(_.root)

        _ <- replaySpace.reset(root1)
        _ <- replayStore.get().isEmpty.map(_ shouldBe true)

        _ <- space.reset(root0)
        _ <- store.get().isEmpty.map(_ shouldBe true)
      } yield ()
  }

  "Creating a COMM Event" should "replay correctly" in
    fixture { (store, replayStore, space, replaySpace) =>
      val channels     = List("ch1")
      val patterns     = List(Wildcard)
      val continuation = "continuation"
      val datum        = "datum1"

      for {
        emptyPoint <- space.createCheckpoint()

        resultConsume <- space.consume(channels, patterns, continuation, false)
        resultProduce <- space.produce(channels(0), datum, false)
        rigPoint      <- space.createCheckpoint()

        _ = resultConsume shouldBe None
        _ = resultProduce shouldBe Some(
          (ContResult(continuation, false, channels, patterns, 1), List(Result(datum, false)))
        )

        _ <- replaySpace.resetAndRig(emptyPoint.root, rigPoint.log)

        replayResultConsume <- replaySpace.consume(channels, patterns, continuation, false)
        replayResultProduce <- replaySpace.produce(channels(0), datum, false)
        finalPoint          <- space.createCheckpoint()

        _ = replayResultConsume shouldBe None
        _ = replayResultProduce shouldBe resultProduce
        _ = finalPoint.root shouldBe rigPoint.root
        _ = replaySpace.replayData shouldBe empty
      } yield ()
    }

  "Picking n datums from m waiting datums" should "replay correctly" in forAll(arbitraryRangesSize) {
    case (n: Int, m: Int) =>
      fixture { (store, replayStore, space, replaySpace) =>
        for {
          emptyPoint <- space.createCheckpoint()

          range = (n until m).toList

          _ <- produceMany(
                space,
                range,
                channelCreator = kp("ch1"),
                datumCreator = i => s"datum$i",
                persist = false
              )
          results <- consumeMany(
                      space,
                      range,
                      channelsCreator = kp(List("ch1")),
                      patterns = List(Wildcard),
                      continuationCreator = i => s"continuation$i",
                      persist = false
                    )
          rigPoint <- space.createCheckpoint()

          _ <- replaySpace.resetAndRig(emptyPoint.root, rigPoint.log)

          _ <- produceMany(
                replaySpace,
                range,
                channelCreator = kp("ch1"),
                datumCreator = i => s"datum$i",
                persist = false
              )
          replayResults <- consumeMany(
                            replaySpace,
                            range,
                            channelsCreator = kp(List("ch1")),
                            patterns = List(Wildcard),
                            continuationCreator = i => s"continuation$i",
                            persist = false
                          )
          finalPoint <- replaySpace.createCheckpoint()

          _ = replayResults should contain theSameElementsAs results
          _ = finalPoint.root shouldBe rigPoint.root
          _ = replaySpace.replayData shouldBe empty
        } yield ()
      }
  }

  "Picking n datums from m persistent waiting datums" should "replay correctly" in forAll(
    arbitraryRangesSize
  ) {
    case (n: Int, m: Int) =>
      fixture { (store, replayStore, space, replaySpace) =>
        for {
          emptyPoint <- space.createCheckpoint()

          range = (n until m).toList

          _ <- produceMany(
                space,
                range,
                channelCreator = kp("ch1"),
                datumCreator = i => s"datum$i",
                persist = true
              )
          results <- consumeMany(
                      space,
                      range,
                      channelsCreator = kp(List("ch1")),
                      patterns = List(Wildcard),
                      continuationCreator = i => s"continuation$i",
                      persist = false
                    )
          rigPoint <- space.createCheckpoint()

          _ <- replaySpace.resetAndRig(emptyPoint.root, rigPoint.log)

          _ <- produceMany(
                replaySpace,
                range,
                channelCreator = kp("ch1"),
                datumCreator = i => s"datum$i",
                persist = true
              )
          replayResults <- consumeMany(
                            replaySpace,
                            range,
                            channelsCreator = kp(List("ch1")),
                            patterns = List(Wildcard),
                            continuationCreator = i => s"continuation$i",
                            persist = false
                          )
          finalPoint <- replaySpace.createCheckpoint()

          _ = replayResults should contain theSameElementsAs results
          _ = finalPoint.root shouldBe rigPoint.root
          _ = replaySpace.replayData shouldBe empty
        } yield ()
      }
  }

  "Picking n continuations from m waiting continuations" should "replay correctly" in forAll(
    arbitraryRangesSize
  ) {
    case (n: Int, m: Int) =>
      fixture { (store, replayStore, space, replaySpace) =>
        for {
          emptyPoint <- space.createCheckpoint()

          range = (n until m).toList

          _ <- consumeMany(
                space,
                range,
                channelsCreator = kp(List("ch1")),
                patterns = List(Wildcard),
                continuationCreator = i => s"continuation$i",
                persist = false
              )
          results <- produceMany(
                      space,
                      range,
                      channelCreator = kp("ch1"),
                      datumCreator = i => s"datum$i",
                      persist = false
                    )
          rigPoint <- space.createCheckpoint()

          _ <- replaySpace.resetAndRig(emptyPoint.root, rigPoint.log)

          _ <- consumeMany(
                replaySpace,
                range,
                channelsCreator = kp(List("ch1")),
                patterns = List(Wildcard),
                continuationCreator = i => s"continuation$i",
                persist = false
              )
          replayResults <- produceMany(
                            replaySpace,
                            range,
                            channelCreator = kp("ch1"),
                            datumCreator = i => s"datum$i",
                            persist = false
                          )
          finalPoint <- replaySpace.createCheckpoint()

          _ = replayResults should contain theSameElementsAs results
          _ = finalPoint.root shouldBe rigPoint.root
          _ = replaySpace.replayData shouldBe empty
        } yield ()
      }
  }

  "Picking n continuations from m persistent waiting continuations" should "replay correctly" in forAll(
    arbitraryRangesSize
  ) {
    case (n: Int, m: Int) =>
      fixture { (store, replayStore, space, replaySpace) =>
        for {
          emptyPoint <- space.createCheckpoint()

          range = (n until m).toList

          _ <- consumeMany(
                space,
                range,
                channelsCreator = kp(List("ch1")),
                patterns = List(Wildcard),
                continuationCreator = i => s"continuation$i",
                persist = true
              )
          results <- produceMany(
                      space,
                      range,
                      channelCreator = kp("ch1"),
                      datumCreator = i => s"datum$i",
                      persist = false
                    )
          rigPoint <- space.createCheckpoint()

          _ <- replaySpace.resetAndRig(emptyPoint.root, rigPoint.log)

          _ <- consumeMany(
                replaySpace,
                range,
                channelsCreator = kp(List("ch1")),
                patterns = List(Wildcard),
                continuationCreator = i => s"continuation$i",
                persist = true
              )
          replayResults <- produceMany(
                            replaySpace,
                            range,
                            channelCreator = kp("ch1"),
                            datumCreator = i => s"datum$i",
                            persist = false
                          )
          finalPoint <- replaySpace.createCheckpoint()

          _ = replayResults should contain theSameElementsAs results
          _ = finalPoint.root shouldBe rigPoint.root
          _ = replaySpace.replayData shouldBe empty
        } yield ()
      }
  }

  "Pick n continuations from m waiting continuations stored at two channels" should "replay correctly" in forAll(
    arbitraryRangesSize
  ) {
    case (n: Int, m: Int) =>
      fixture { (store, replayStore, space, replaySpace) =>
        for {
          emptyPoint <- space.createCheckpoint()

          range = (n until m).toList

          _ <- consumeMany(
                space,
                range,
                channelsCreator = kp(List("ch1", "ch2")),
                patterns = List(Wildcard, Wildcard),
                continuationCreator = i => s"continuation$i",
                persist = false
              )
          _ <- produceMany(
                space,
                range,
                channelCreator = kp("ch1"),
                datumCreator = kp("datum1"),
                persist = false
              )
          results <- produceMany(
                      space,
                      range,
                      channelCreator = kp("ch2"),
                      datumCreator = kp("datum2"),
                      persist = false
                    )
          rigPoint <- space.createCheckpoint()

          _ <- replaySpace.resetAndRig(emptyPoint.root, rigPoint.log)

          _ <- consumeMany(
                replaySpace,
                range,
                channelsCreator = kp(List("ch1", "ch2")),
                patterns = List(Wildcard, Wildcard),
                continuationCreator = i => s"continuation$i",
                persist = false
              )
          _ <- produceMany(
                replaySpace,
                range,
                channelCreator = kp("ch1"),
                datumCreator = kp("datum1"),
                persist = false
              )
          replayResults <- produceMany(
                            replaySpace,
                            range,
                            channelCreator = kp("ch2"),
                            datumCreator = kp("datum2"),
                            persist = false
                          )
          finalPoint <- replaySpace.createCheckpoint()

          _ = replayResults should contain theSameElementsAs results
          _ = finalPoint.root shouldBe rigPoint.root
          _ = replaySpace.replayData shouldBe empty
        } yield ()
      }
  }

  "Picking n datums from m waiting datums while doing a bunch of other junk" should "replay correctly" in forAll(
    arbitraryRangesSize
  ) {
    case (n: Int, m: Int) =>
      fixture { (store, replayStore, space, replaySpace) =>
        for {
          emptyPoint <- space.createCheckpoint()

          _ <- produceMany(
                space,
                range = n until m toList,
                channelCreator = kp("ch1"),
                datumCreator = i => s"datum$i",
                persist = false
              )
          _ <- consumeMany(
                space,
                range = (m + 1 until m + 10).toList,
                channelsCreator = i => List(s"ch$i"),
                patterns = List(Wildcard),
                continuationCreator = i => s"continuation$i",
                persist = false
              )
          _ <- produceMany(
                space,
                range = m + 11 until m + 20 toList,
                channelCreator = i => s"ch$i",
                datumCreator = i => s"datum$i",
                persist = false
              )
          results <- consumeMany(
                      space,
                      range = (n until m).toList,
                      channelsCreator = kp(List("ch1")),
                      patterns = List(Wildcard),
                      continuationCreator = i => s"continuation$i",
                      persist = false
                    )
          rigPoint <- space.createCheckpoint()

          _ <- replaySpace.resetAndRig(emptyPoint.root, rigPoint.log)

          _ <- produceMany(
                replaySpace,
                range = n until m toList,
                channelCreator = kp("ch1"),
                datumCreator = i => s"datum$i",
                persist = false
              )
          _ <- consumeMany(
                replaySpace,
                range = m + 1 until m + 10 toList,
                channelsCreator = i => List(s"ch$i"),
                patterns = List(Wildcard),
                continuationCreator = i => s"continuation$i",
                persist = false
              )
          _ <- produceMany(
                replaySpace,
                range = m + 11 until m + 20 toList,
                channelCreator = i => s"ch$i",
                datumCreator = i => s"datum$i",
                persist = false
              )
          replayResults <- consumeMany(
                            replaySpace,
                            range = n until m toList,
                            channelsCreator = kp(List("ch1")),
                            patterns = List(Wildcard),
                            continuationCreator = i => s"continuation$i",
                            persist = false
                          )
          finalPoint <- replaySpace.createCheckpoint()

          _ = replayResults should contain theSameElementsAs results
          _ = finalPoint.root shouldBe rigPoint.root
          _ = replaySpace.replayData shouldBe empty
        } yield ()
      }
  }

  "Picking n continuations from m persistent waiting continuations while doing a bunch of other junk" should "replay correctly" in forAll(
    arbitraryRangesSize
  ) {
    case (n: Int, m: Int) =>
      fixture { (store, replayStore, space, replaySpace) =>
        for {
          emptyPoint <- space.createCheckpoint()

          _ <- consumeMany(
                space,
                range = n until m toList,
                channelsCreator = i => List(s"ch$i"),
                patterns = List(Wildcard),
                continuationCreator = i => s"continuation$i",
                persist = false
              )
          _ <- produceMany(
                space,
                range = m + 1 until m + 10 toList,
                channelCreator = kp("ch1"),
                datumCreator = i => s"datum$i",
                persist = false
              )
          _ <- consumeMany(
                space,
                range = m + 11 until m + 20 toList,
                channelsCreator = kp(List("ch1")),
                patterns = List(Wildcard),
                continuationCreator = i => s"continuation$i",
                persist = false
              )
          results <- produceMany(
                      space,
                      range = n until m,
                      channelCreator = i => s"ch$i",
                      datumCreator = i => s"datum$i",
                      persist = false
                    )
          rigPoint <- space.createCheckpoint()

          _ <- replaySpace.resetAndRig(emptyPoint.root, rigPoint.log)

          _ <- consumeMany(
                replaySpace,
                range = n until m toList,
                channelsCreator = i => List(s"ch$i"),
                patterns = List(Wildcard),
                continuationCreator = i => s"continuation$i",
                persist = false
              )
          _ <- produceMany(
                replaySpace,
                range = m + 1 until m + 10 toList,
                channelCreator = kp("ch1"),
                datumCreator = i => s"datum$i",
                persist = false
              )
          _ <- consumeMany(
                replaySpace,
                range = m + 11 until m + 20 toList,
                channelsCreator = kp(List("ch1")),
                patterns = List(Wildcard),
                continuationCreator = i => s"continuation$i",
                persist = false
              )
          replayResults <- produceMany(
                            replaySpace,
                            range = n until m toList,
                            channelCreator = i => s"ch$i",
                            datumCreator = i => s"datum$i",
                            persist = false
                          )
          finalPoint <- replaySpace.createCheckpoint()

          _ = replayResults should contain theSameElementsAs results
          _ = finalPoint.root shouldBe rigPoint.root
          _ = replaySpace.replayData shouldBe empty
        } yield ()
      }
  }

  "Replay rspace" should "correctly remove things from replay data" in fixture {
    (store, replayStore, space, replaySpace) =>
      val channels = List("ch1")
      val patterns = List[Pattern](Wildcard)
      val k        = "continuation"
      val datum    = "datum"
      for {
        emptyPoint <- space.createCheckpoint()

        cr = Consume.create(channels, patterns, k, persist = false)

        _ <- consumeMany(
              space,
              range = 0 to 1 toList,
              channelsCreator = kp(channels),
              patterns = patterns,
              continuationCreator = kp(k),
              persist = false
            )
        _ <- produceMany(
              space,
              range = 0 to 1 toList,
              channelCreator = kp(channels(0)),
              datumCreator = kp(datum),
              persist = false
            )
        rigPoint <- space.createCheckpoint()

        _ <- replaySpace.resetAndRig(emptyPoint.root, rigPoint.log)

        _ = replaySpace.replayData.get(cr).map(_.size).value shouldBe 2

        _ <- replaySpace.consume(channels, patterns, k, persist = false)
        _ <- replaySpace.consume(channels, patterns, k, persist = false)
        _ <- replaySpace.produce(channels(0), datum, persist = false)

        _ = replaySpace.replayData.get(cr).map(_.size).value shouldBe 1

        _ <- replaySpace.produce(channels(0), datum, persist = false)
        _ = replaySpace.replayData.get(cr) shouldBe None
      } yield ()
  }

  "producing" should "return same, stable checkpoint root hashes" in forAll(arbitraryRangeSize) {
    n: Int =>
      def process(indices: Seq[Int]): Checkpoint = fixture {
        (store, replayStore, space, replaySpace) =>
          Task.delay {
            for (i <- indices) {
              replaySpace.produce("ch1", s"datum$i", false).unsafeRunSync
            }
            space.createCheckpoint().unsafeRunSync
          }
      }

      val cp1 = process(0 to n)
      val cp2 = process(n to 0 by -1)
      cp1.root shouldBe cp2.root
  }

  "an install" should "be available after resetting to a checkpoint" in fixture {
    (store, replayStore, space, replaySpace) =>
      val channel      = "ch1"
      val datum        = "datum1"
      val key          = List(channel)
      val patterns     = List(Wildcard)
      val continuation = "continuation"

      for {
        _ <- space.install(key, patterns, continuation)
        _ <- replaySpace.install(key, patterns, continuation)

        produce1     <- space.produce(channel, datum, persist = false)
        _            = produce1 shouldBe defined
        afterProduce <- space.createCheckpoint()

        _ <- replaySpace.resetAndRig(afterProduce.root, afterProduce.log)

        produce2 <- replaySpace.produce(channel, datum, persist = false)
        _        = produce2 shouldBe defined
      } yield ()
  }

  "reset" should
    """|empty the replay store,
       |reset the replay trie updates log,
       |and reset the replay data""".stripMargin in
    fixture { (_, replayStore, space, replaySpace) =>
      val channels     = List("ch1")
      val patterns     = List(Wildcard)
      val continuation = "continuation"

      for {
        emptyPoint <- space.createCheckpoint()

        consume1 <- space.consume(channels, patterns, continuation, false)
        _        = consume1 shouldBe None

        rigPoint <- space.createCheckpoint()

        _ <- replaySpace.resetAndRig(emptyPoint.root, rigPoint.log)

        consume2 <- replaySpace.consume(channels, patterns, continuation, false)
        _        = consume2 shouldBe None

        _ <- replayStore.get().isEmpty.map(_ shouldBe false)
        _ <- replayStore
              .get()
              .changes()
              .map(collectActions[InsertContinuations[String, Pattern, String]])
              .map(_.length shouldBe 1)

        _ <- replaySpace.reset(emptyPoint.root)
        _ <- replayStore.get().isEmpty.map(_ shouldBe true)
        _ = replaySpace.replayData shouldBe empty

        checkpoint1 <- replaySpace.createCheckpoint()
        _           = checkpoint1.log shouldBe empty
      } yield ()
    }

  "clear" should
    """|empty the replay store,
       |reset the replay event log,
       |reset the replay trie updates log,
       |and reset the replay data""".stripMargin in
    fixture { (store, replayStore, space, replaySpace) =>
      val channels     = List("ch1")
      val patterns     = List(Wildcard)
      val continuation = "continuation"

      for {
        emptyPoint <- space.createCheckpoint()

        consume1 <- space.consume(channels, patterns, continuation, false)
        _        = consume1 shouldBe None

        rigPoint <- space.createCheckpoint()

        _ <- replaySpace.resetAndRig(emptyPoint.root, rigPoint.log)

        consume2 <- replaySpace.consume(channels, patterns, continuation, false)
        _        = consume2 shouldBe None

        _ <- replayStore.get().isEmpty.map(_ shouldBe false)
        _ <- replayStore
              .get()
              .changes()
              .map(collectActions[InsertContinuations[String, Pattern, String]])
              .map(_.length shouldBe 1)

        checkpoint0 <- replaySpace.createCheckpoint()
        _           = checkpoint0.log shouldBe empty // we don't record trace logs in ReplayRspace

        _ <- replaySpace.clear()
        _ = replayStore.get().isEmpty.map(_ shouldBe true)
        _ = replaySpace.replayData shouldBe empty

        checkpoint1 <- replaySpace.createCheckpoint()
        _           = checkpoint1.log shouldBe empty
      } yield ()
    }

  "replay" should "not allow for ambiguous executions" in fixture {
    (store, replayStore, space, replaySpace) =>
      val noMatch                 = None
      val channel1                = "ch1"
      val channel2                = "ch2"
      val key1                    = List(channel1, channel2)
      val patterns: List[Pattern] = List(Wildcard, Wildcard)
      val continuation1           = "continuation1"
      val continuation2           = "continuation2"
      val data1                   = "datum1"
      val data2                   = "datum2"
      val data3                   = "datum3"

      implicit class AnyShouldF[F[_]: Functor, T](leftSideValue: F[T]) {
        def shouldBeF(value: T): F[Assertion] =
          leftSideValue.map(_ shouldBe value)

        def shouldNotBeF(value: T): F[Assertion] =
          leftSideValue.map(_ should not be value)
      }

      for {
        emptyCh <- space.createCheckpoint()
        _       <- space.produce(channel1, data3, false, 0) shouldBeF noMatch
        _       <- space.produce(channel1, data3, false, 0) shouldBeF noMatch
        _       <- space.produce(channel2, data1, false, 0) shouldBeF noMatch

        _ <- space
              .consume(key1, patterns, continuation1, false, 0) shouldNotBeF Option.empty
        //continuation1 produces data1 on ch2
        _ <- space.produce(channel2, data1, false, 1) shouldBeF noMatch
        _ <- space
              .consume(key1, patterns, continuation2, false, 0) shouldNotBeF Option.empty
        //continuation2 produces data2 on ch2
        _         <- space.produce(channel2, data2, false, 2) shouldBeF noMatch
        afterPlay <- space.createCheckpoint()

        //rig
        _ <- replaySpace.resetAndRig(emptyCh.root, afterPlay.log)

        _ <- replaySpace.produce(channel1, data3, false, 0) shouldBeF noMatch
        _ <- replaySpace.produce(channel1, data3, false, 0) shouldBeF noMatch
        _ <- replaySpace.produce(channel2, data1, false, 0) shouldBeF noMatch
        _ <- replaySpace.consume(key1, patterns, continuation2, false, 0) shouldBeF noMatch

        _ <- replaySpace
              .consume(key1, patterns, continuation1, false, 0) shouldNotBeF Option.empty
        //continuation1 produces data1 on ch2
        _ <- replaySpace
              .produce(channel2, data1, false, 1) shouldNotBeF Option.empty //matches continuation2
        //continuation2 produces data2 on ch2
        _ <- replaySpace.produce(channel2, data2, false, 1) shouldBeF noMatch

        _ = replaySpace.replayData.isEmpty shouldBe true
      } yield ()
  }
}

trait ReplayRSpaceTestsBase[C, P, A, K]
    extends FlatSpec
    with Matchers
    with OptionValues
    with PropertyChecks {
  val logger = Logger(this.getClass.getName.stripSuffix("$"))

  implicit override val generatorDrivenConfig =
    PropertyCheckConfiguration(minSize = 10, minSuccessful = 5)

  override def withFixture(test: NoArgTest): Outcome = {
    logger.debug(s"Test: ${test.name}")
    super.withFixture(test)
  }

  def fixture[S](
      f: (
          AtomicAny[HotStore[Task, C, P, A, K]],
          AtomicAny[HotStore[Task, C, P, A, K]],
          ISpace[Task, C, P, A, A, K],
          IReplaySpace[Task, C, P, A, A, K]
      ) => Task[S]
  )(
      implicit
      sc: Serialize[C],
      sp: Serialize[P],
      sa: Serialize[A],
      sk: Serialize[K]
  ): S
}

trait InMemoryReplayRSpaceTestsBase[C, P, A, K] extends ReplayRSpaceTestsBase[C, P, A, K] {
  import SchedulerPools.global
  override def fixture[S](
      f: (
          AtomicAny[HotStore[Task, C, P, A, K]],
          AtomicAny[HotStore[Task, C, P, A, K]],
          ISpace[Task, C, P, A, A, K],
          IReplaySpace[Task, C, P, A, A, K]
      ) => Task[S]
  )(
      implicit
      sc: Serialize[C],
      sp: Serialize[P],
      sa: Serialize[A],
      sk: Serialize[K]
  ): S = {
    implicit val log: Log[Task]          = Log.log[Task]
    implicit val metricsF: Metrics[Task] = new Metrics.MetricsNOP[Task]()

    val branch = Branch("inmem")

    val dbDir: Path   = Files.createTempDirectory("replayrspace-test-")
    val mapSize: Long = 1024L * 1024L * 1024L

    def storeConfig(name: String): StoreConfig =
      StoreConfig(
        Files.createDirectories(dbDir.resolve(name)),
        mapSize,
        2,
        2048,
        List(EnvFlags.MDB_NOTLS)
      )

    val config = LMDBRSpaceStorageConfig(
      storeConfig("cold"),
      storeConfig("history"),
      storeConfig("roots")
    )

    implicit val cc = sc.toCodec
    implicit val cp = sp.toCodec
    implicit val ca = sa.toCodec
    implicit val ck = sk.toCodec

    (for {
      historyRepository <- HistoryRepositoryInstances.lmdbRepository[Task, C, P, A, K](config)
      cache <- Cell.refCell[Task, Cache[C, P, A, K]](
                Cache[C, P, A, K]()
              )
      store = {
        implicit val hr = historyRepository
        implicit val c  = cache
        AtomicAny(HotStore.inMem[Task, C, P, A, K])
      }
      space = new RSpace[Task, C, P, A, A, K](
        historyRepository,
        store,
        branch
      )
      historyCache <- Cell.refCell[Task, Cache[C, P, A, K]](
                       Cache[C, P, A, K]()
                     )
      replayStore = {
        implicit val hr = historyRepository
        implicit val c  = historyCache
        AtomicAny(HotStore.inMem[Task, C, P, A, K])
      }
      replaySpace = new ReplayRSpace[Task, C, P, A, A, K](
        historyRepository,
        replayStore,
        branch
      )
      res <- f(store, replayStore, space, replaySpace)
      _   <- Sync[Task].delay(dbDir.recursivelyDelete())
    } yield { res }).unsafeRunSync
  }
}

class InMemoryReplayRSpaceTests
    extends InMemoryReplayRSpaceTestsBase[String, Pattern, String, String]
    with ReplayRSpaceTests
