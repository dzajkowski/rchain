package coop.rchain.rspace

import coop.rchain.rspace.history.Trie

trait Introspecter {
  def magic[TK, TV](value: Trie[TK, TV]): Unit
}

object IntrospecterInstances {
  def noop() = new Introspecter {
    override def magic[TK, TV](value: Trie[TK, TV]): Unit = ()
  }
}
