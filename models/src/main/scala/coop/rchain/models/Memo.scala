package coop.rchain.models
import monix.eval.Coeval
import monix.eval.Coeval.Eager

class Memo[A](f: => Coeval[A]) {

  private[this] var thunk             = f
  private[this] var result: Coeval[A] = _

  def get: Coeval[A] = synchronized {
    result match {
      case e: Eager[A] => e
      case _ =>
        Coeval.defer {
          synchronized {
            result match {
              case e: Eager[A] => e
              case _ =>
                thunk.map { r =>
                  thunk = null //allow GC-ing the thunk
                  result = Coeval.now(r)
                  r
                }
            }
          }
        }
    }
  }
}
