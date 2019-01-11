package coop.rchain.casper.util.rholang
import cats.effect.Resource
import coop.rchain.catscontrib.TaskContrib._
import coop.rchain.rholang.Resources.mkRuntime
import coop.rchain.shared.StoreType
import monix.eval.Task
import monix.execution.Scheduler

object Resources {
  def mkRuntimeManager(
      prefix: String,
      storageSize: Int = 1024 * 1024,
      storeType: StoreType = StoreType.LMDB
  )(implicit scheduler: Scheduler): Resource[Task, RuntimeManager[Task]] =
    mkRuntime(prefix)
      .flatMap { runtime =>
        Resource.pure(RuntimeManager.fromRuntime(runtime).unsafeRunSync)
      }
}
