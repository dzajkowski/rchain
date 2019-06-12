package coop.rchain.rspace.pure

import cats.effect.Sync
import coop.rchain.rspace.ISpace.Channel.consumed
import coop.rchain.rspace._

trait PureRSpace[F[_], C, P, A, R, K] {
  def consume(
      channels: Seq[C],
      patterns: Seq[P],
      continuation: K,
      persist: Boolean,
      sequenceNumber: Int = 0
  ): F[Option[(ContResult[C, P, K], Seq[Result[R]])]]

  def install(channels: Seq[C], patterns: Seq[P], continuation: K): F[Option[(K, Seq[R])]]

  def produce(
      channel: C,
      data: A,
      persist: Boolean,
      sequenceNumber: Int = 0
  ): F[Option[(ContResult[C, P, K], Seq[Result[R]])]]

  def createCheckpoint(): F[Checkpoint]

  def reset(hash: Blake2b256Hash): F[Unit]

  def close(): F[Unit]
}

object PureRSpace {
  def apply[F[_]](implicit F: Sync[F]): PureRSpaceApplyBuilders[F] = new PureRSpaceApplyBuilders(F)

  final class PureRSpaceApplyBuilders[F[_]](val F: Sync[F]) extends AnyVal {
    def of[C, P, A, R, K](
        space: ISpace[F, C, P, A, R, K]
    )(implicit mat: Match[F, P, A, R]): PureRSpace[F, C, P, A, R, K] =
      new PureRSpace[F, C, P, A, R, K] {
        def consume(
            channels: Seq[C],
            patterns: Seq[P],
            continuation: K,
            persist: Boolean,
            sequenceNumber: Int
        ): F[Option[(ContResult[C, P, K], Seq[Result[R]])]] =
          space.consume(channels.map(consumed), patterns, continuation, persist, sequenceNumber)

        def install(channels: Seq[C], patterns: Seq[P], continuation: K): F[Option[(K, Seq[R])]] =
          space.install(channels, patterns, continuation)

        def produce(
            channel: C,
            data: A,
            persist: Boolean,
            sequenceNumber: Int
        ): F[Option[(ContResult[C, P, K], Seq[Result[R]])]] =
          space.produce(channel, data, persist, sequenceNumber)

        def createCheckpoint(): F[Checkpoint] = space.createCheckpoint()

        def reset(hash: Blake2b256Hash): F[Unit] = space.reset(hash)

        def close(): F[Unit] = space.close()
      }
  }
}
