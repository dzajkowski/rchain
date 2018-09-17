package coop.rchain.rspace.concurrent

trait TwoStepLock[K] {
  def acquire[R, S, W](keysA: Seq[K])(phaseTwo: () => Seq[K])(thunk: => W)(
      implicit o: Ordering[K]): W
}

class DefaultTwoStepLock[K] extends TwoStepLock[K] {
  private[this] val phaseA: MultiLock[K] = new DefaultMultiLock[K]
  private[this] val phaseB: MultiLock[K] = new DefaultMultiLock[K]

  override def acquire[R, S, W](keysA: Seq[K])(phaseTwo: () => Seq[K])(thunk: => W)(
      implicit o: Ordering[K]): W = {
    println("acquire keys a", keysA)
    phaseA.acquire(keysA) {
      val keysB = phaseTwo
      println("acquire keys b", keysA)
      phaseB.acquire(keysB()) {
        val x = thunk
        println("releasing keys a b", keysA)
        x
      }
    }
  }
}
