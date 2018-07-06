package coop.rchain.rspace

import coop.rchain.rspace.examples.StringExamples.{Pattern, StringsCaptor, Wildcard}
import coop.rchain.rspace.examples.StringExamples.implicits._
import coop.rchain.rspace.internal._

trait JoinOperationsTests extends StorageTestsBase[String, Pattern, String, StringsCaptor] {

  "joins" should "remove joins if no PsK" in withTestSpace { space =>
    space.storeTransactional.withTxn(space.storeTransactional.createTxnWrite()) { txn =>
      space.store.putDatum(txn, List("ch1"), Datum.create("ch1", "datum1", persist = false))
      space.store.putDatum(txn, List("ch2"), Datum.create("ch2", "datum2", persist = false))
      space.store.addJoin(txn, "ch1", List("ch1", "ch2"))
      space.store.addJoin(txn, "ch2", List("ch1", "ch2"))

      //ensure that doubled addJoin creates only one entry
      space.store.addJoin(txn, "ch1", List("ch1", "ch2"))
      space.store.addJoin(txn, "ch2", List("ch1", "ch2"))
    }

    space.storeTransactional.withTxn(space.storeTransactional.createTxnRead()) { txn =>
      space.store.getJoin(txn, "ch1") shouldBe List(List("ch1", "ch2"))
      space.store.getJoin(txn, "ch2") shouldBe List(List("ch1", "ch2"))
    }

    space.storeTransactional.withTxn(space.storeTransactional.createTxnWrite()) { txn =>
      space.store.removeJoin(txn, "ch1", List("ch1", "ch2"))
      space.store.removeJoin(txn, "ch2", List("ch1", "ch2"))
    }

    space.storeTransactional.withTxn(space.storeTransactional.createTxnRead()) { txn =>
      space.store.getJoin(txn, "ch1") shouldBe List.empty[List[String]]
      space.store.getJoin(txn, "ch2") shouldBe List.empty[List[String]]
    }

    space.store.isEmpty shouldBe false

    //now ensure that garbage-collection works and all joins
    //are removed when we remove As
    space.storeTransactional.withTxn(space.storeTransactional.createTxnWrite()) { txn =>
      space.store.removeDatum(txn, List("ch1"), 0)
      space.store.removeDatum(txn, List("ch2"), 0)
    }

    space.store.isEmpty shouldBe true
  }

}
