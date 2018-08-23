package coop.rchain.rspace.pure

import cats.effect.Sync
import coop.rchain.rspace._

import scala.collection.immutable.Seq
import scala.language.higherKinds

trait PureRSpace[F[_], C, P, A, R, K] {

  def consume(channels: Seq[C], patterns: Seq[P], continuation: K, persist: Boolean)(
      implicit m: Match[P, A, R],
      s: Sync[F]): F[Option[(K, Seq[R])]]

  def install(channels: Seq[C], patterns: Seq[P], continuation: K)(
      implicit m: Match[P, A, R],
      s: Sync[F]): F[Option[(K, Seq[R])]]

  def produce(channel: C, data: A, persist: Boolean)(implicit m: Match[P, A, R],
                                                     s: Sync[F]): F[Option[(K, Seq[R])]]

  def createCheckpoint()(s: Sync[F]): F[Checkpoint]

  def reset(hash: Blake2b256Hash)(s: Sync[F]): F[Unit]

  def close()(s: Sync[F]): F[Unit]
}

object PureRSpace {
  def sync[F[_], C, P, A, R, K](space: ISpace[C, P, A, R, K]): PureRSpace[F, C, P, A, R, K] =
    new SyncPurePSpace[F, C, P, A, R, K](space)

  def async[F[_], C, P, A, R, K]: PureRSpace[F, C, P, A, R, K] = ???
}
