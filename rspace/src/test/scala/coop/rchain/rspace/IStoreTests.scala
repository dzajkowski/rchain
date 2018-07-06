package coop.rchain.rspace

import coop.rchain.rspace.examples.StringExamples._
import coop.rchain.rspace.examples.StringExamples.implicits._
import coop.rchain.rspace.internal._
import coop.rchain.rspace.test.ArbitraryInstances._
import org.scalacheck.Gen
import org.scalactic.anyvals.PosInt
import org.scalatest.AppendedClues
import org.scalatest.prop.GeneratorDrivenPropertyChecks

trait IStoreTests
    extends StorageTestsBase[String, Pattern, String, StringsCaptor]
    with GeneratorDrivenPropertyChecks
    with AppendedClues {

  implicit override val generatorDrivenConfig: PropertyCheckConfiguration =
    PropertyCheckConfiguration(minSuccessful = PosInt(10))

  "putDatum" should "put datum in a new channel" in withTestSpace { space =>
    forAll("channel", "datum") { (channel: String, datumValue: String) =>
      val key   = List(channel)
      val datum = Datum.create(channel, datumValue, false)

      space.transactional.withTxn(space.transactional.createTxnWrite()) { txn =>
        space.store.putDatum(txn, key, datum)
        space.store.getData(txn, key) should contain theSameElementsAs (Seq(datum))
        space.store.clear(txn)
      }
    }
  }

  it should "append datum if channel already exists" in withTestSpace { space =>
    forAll("channel", "datum") { (channel: String, datumValue: String) =>
      val key    = List(channel)
      val datum1 = Datum.create(channel, datumValue, false)
      val datum2 = Datum.create(channel, datumValue + "2", false)

      space.transactional.withTxn(space.transactional.createTxnWrite()) { txn =>
        space.store.putDatum(txn, key, datum1)
        space.store.putDatum(txn, key, datum2)
        space.store.getData(txn, key) should contain theSameElementsAs (Seq(datum1, datum2))
        space.store.clear(txn)
      }
    }
  }

  "hashChannels" should "return same hashes for unordered channels" in withTestSpace { space =>
    val hash12 = space.store.hashChannels(List("ch1", "ch2"))
    val hash21 = space.store.hashChannels(List("ch2", "ch1"))
    hash12 shouldBe hash21
  }

  private[this] val validIndices =
    for (n <- Gen.choose(1, 10)) yield n

  "removeDatum" should s"remove datum at index" in withTestSpace { space =>
    val size = 11
    forAll("channel", "datum", validIndices, minSuccessful(10)) {
      (channel: String, datumValue: String, index: Int) =>
        val key = List(channel)
        val data = List.tabulate(size) { i =>
          Datum.create(channel, datumValue + i, false)
        }

        space.transactional.withTxn(space.transactional.createTxnWrite()) { txn =>
          data.foreach { d =>
            space.store.putDatum(txn, key, d)
          }
          space.store.removeDatum(txn, key, index - 1)
          space.store.getData(txn, key) should contain theSameElementsAs (data.filterNot(
            _.a == datumValue + (size - index)))
          space.store.clear(txn)
        }
    }
  }

  it should "remove obsolete channels" in withTestSpace { space =>
    forAll("channel", "datum") { (channel: String, datum: String) =>
      val key  = List(channel)
      val hash = space.store.hashChannels(key)
      space.transactional.withTxn(space.transactional.createTxnWrite()) { txn =>
        space.store.putDatum(txn, key, Datum.create(channel, datum, persist = false))
        // collectGarbage is called in removeDatum:
        space.store.removeDatum(txn, key, 0)
        space.store.getChannels(txn, hash) shouldBe empty
        space.store.clear(txn)
      }
    }
  }

  "putWaitingContinuation" should "put waiting continuation in a new channel" in withTestSpace {
    space =>
      forAll("channel", "continuation") { (channel: String, pattern: String) =>
        val key          = List(channel)
        val patterns     = List(StringMatch(pattern))
        val continuation = new StringsCaptor
        val wc: WaitingContinuation[Pattern, StringsCaptor] =
          WaitingContinuation.create(key, patterns, continuation, false)

        space.transactional.withTxn(space.transactional.createTxnWrite()) { txn =>
          space.store.putWaitingContinuation(txn, key, wc)
          space.store.getWaitingContinuation(txn, key) shouldBe List(wc)
          space.store.clear(txn)
        }
      }
  }

  it should "append continuation if channel already exists" in withTestSpace { space =>
    forAll("channel", "continuation") { (channel: String, pattern: String) =>
      val key          = List(channel)
      val patterns     = List(StringMatch(pattern))
      val continuation = new StringsCaptor
      val wc1: WaitingContinuation[Pattern, StringsCaptor] =
        WaitingContinuation.create(key, patterns, continuation, false)

      val wc2: WaitingContinuation[Pattern, StringsCaptor] =
        WaitingContinuation.create(key, List(StringMatch(pattern + 2)), continuation, false)

      space.transactional.withTxn(space.transactional.createTxnWrite()) { txn =>
        space.store.putWaitingContinuation(txn, key, wc1)
        space.store.putWaitingContinuation(txn, key, wc2)
        space.store.getWaitingContinuation(txn, key) should contain theSameElementsAs List(wc1, wc2)
        space.store.clear(txn)
      }
    }
  }

  "removeWaitingContinuation" should "remove waiting continuation from index" in withTestSpace {
    space =>
      forAll("channel", "continuation") { (channel: String, pattern: String) =>
        val key          = List(channel)
        val patterns     = List(StringMatch(pattern))
        val continuation = new StringsCaptor
        val wc1: WaitingContinuation[Pattern, StringsCaptor] =
          WaitingContinuation.create(key, patterns, continuation, false)
        val wc2: WaitingContinuation[Pattern, StringsCaptor] =
          WaitingContinuation.create(key, List(StringMatch(pattern + 2)), continuation, false)

        space.transactional.withTxn(space.transactional.createTxnWrite()) { txn =>
          space.store.putWaitingContinuation(txn, key, wc1)
          space.store.putWaitingContinuation(txn, key, wc2)
          space.store.removeWaitingContinuation(txn, key, 0)
          space.store.getWaitingContinuation(txn, key) shouldBe List(wc1)
          space.store.clear(txn)
        }
      }
  }

  "addJoin" should "add join for a channel" in withTestSpace { space =>
    forAll("channel", "channels") { (channel: String, channels: List[String]) =>
      space.transactional.withTxn(space.transactional.createTxnWrite()) { txn =>
        space.store.addJoin(txn, channel, channels)
        space.store.getJoin(txn, channel) shouldBe List(channels)
        space.store.clear(txn)
      }
    }
  }

  "removeJoin" should "remove join for a channel" in withTestSpace { space =>
    forAll("channel", "channels") { (channel: String, channels: List[String]) =>
      space.transactional.withTxn(space.transactional.createTxnWrite()) { txn =>
        space.store.addJoin(txn, channel, channels)
        space.store.removeJoin(txn, channel, channels)
        space.store.getJoin(txn, channel) shouldBe empty
        space.store.clear(txn)
      }
    }
  }

  it should "remove only passed in joins for a channel" in withTestSpace { space =>
    forAll("channel", "channels") { (channel: String, channels: List[String]) =>
      space.transactional.withTxn(space.transactional.createTxnWrite()) { txn =>
        space.store.addJoin(txn, channel, channels)
        space.store.addJoin(txn, channel, List("otherChannel"))
        space.store.removeJoin(txn, channel, channels)
        space.store.getJoin(txn, channel) shouldBe List(List("otherChannel"))
        space.store.clear(txn)
      }
    }
  }

  "collapse" should "work on empty history" in withTestSpace { space =>
    space.store.collapse(List.empty) shouldBe List.empty
  }

  it should "return unmodified history when nothing to prune" in withTestSpace { space =>
    forAll("gnat") { (gnat: GNAT[String, Pattern, String, StringsCaptor]) =>
      gnat.wks
        .map(_.patterns.size)
        .distinct should contain only gnat.channels.size withClue "#patterns in each continuation should equal #channels"

      val history = List(TrieUpdate(0, Insert, space.store.hashChannels(gnat.channels), gnat))
      space.store.collapse(history) shouldBe history
    }
  }

  it should "return unmodified history when nothing to prune in multiple gnats" in withTestSpace {
    space =>
      forAll(distinctListOf[GNAT[String, Pattern, String, StringsCaptor]], SizeRange(3)) {
        (gnats: Seq[GNAT[String, Pattern, String, StringsCaptor]]) =>
          val history = gnats
            .flatMap(gnat =>
              List(TrieUpdate(0, Insert, space.store.hashChannels(gnat.channels), gnat)))
            .toList
          space.store.collapse(history) should contain theSameElementsAs (history)
      }
  }

  it should "remove all operations from history with the same hash when last operation is delete" in withTestSpace {
    space =>
      forAll("gnat1", "gnat2") {
        (gnat1: GNAT[String, Pattern, String, StringsCaptor],
         gnat2: GNAT[String, Pattern, String, StringsCaptor]) =>
          val gnat1Ops =
            List(TrieUpdate(0, Insert, space.store.hashChannels(gnat1.channels), gnat1),
                 TrieUpdate(1, Delete, space.store.hashChannels(gnat1.channels), gnat1))
          val gnat2Ops =
            List(TrieUpdate(2, Insert, space.store.hashChannels(gnat2.channels), gnat2))
          val history = gnat1Ops ++ gnat2Ops
          space.store.collapse(history) shouldBe gnat2Ops
      }
  }

  it should "remove all operations from history with the same hash when last operation is delete - longer case with same hash" in withTestSpace {
    space =>
      forAll("gnat1") { (gnat1: GNAT[String, Pattern, String, StringsCaptor]) =>
        val gnatOps = List(
          TrieUpdate(0, Insert, space.store.hashChannels(gnat1.channels), gnat1),
          TrieUpdate(1, Insert, space.store.hashChannels(gnat1.channels), gnat1),
          TrieUpdate(2, Insert, space.store.hashChannels(gnat1.channels), gnat1),
          TrieUpdate(3, Delete, space.store.hashChannels(gnat1.channels), gnat1),
        )
        space.store.collapse(gnatOps) shouldBe empty
      }
  }

  it should "remove all but the last operation from history with the same hash when last operation is insert" in withTestSpace {
    space =>
      forAll("gnat") { (gnat: GNAT[String, Pattern, String, StringsCaptor]) =>
        val lastInsert = TrieUpdate(1, Insert, space.store.hashChannels(gnat.channels), gnat)

        val history =
          List(TrieUpdate(0, Insert, space.store.hashChannels(gnat.channels), gnat), lastInsert)
        space.store.collapse(history) shouldBe List(lastInsert)
      }
  }

  it should "remove all but the last operation from history with the same hash when operation with largest count is insert" in withTestSpace {
    space =>
      forAll("gnat") { (gnat: GNAT[String, Pattern, String, StringsCaptor]) =>
        val lastInsert = TrieUpdate(2, Insert, space.store.hashChannels(gnat.channels), gnat)

        val history = List(TrieUpdate(0, Insert, space.store.hashChannels(gnat.channels), gnat),
                           lastInsert,
                           TrieUpdate(1, Delete, space.store.hashChannels(gnat.channels), gnat))
        space.store.collapse(history) shouldBe List(lastInsert)
      }
  }
}

class InMemoryStoreTests extends InMemoryStoreTestsBase with IStoreTests
class LMDBStoreTests     extends LMDBStoreTestsBase with IStoreTests
