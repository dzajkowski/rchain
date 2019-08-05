package coop.rchain.node.api

import cats._
import cats.data._
import cats.implicits._
import cats.effect.concurrent.Semaphore
import cats.effect.Concurrent
import cats.mtl.implicits._
import coop.rchain.blockstorage.BlockStore
import coop.rchain.casper.engine._
import EngineCell._
import coop.rchain.casper.SafetyOracle
import coop.rchain.casper.api._
import coop.rchain.casper.protocol.{DeployData, _}
import coop.rchain.catscontrib.Catscontrib._
import coop.rchain.catscontrib.{TaskContrib, Taskable}
import coop.rchain.catscontrib.TaskContrib._
import coop.rchain.either.{Either => GrpcEither}
import coop.rchain.graphz._
import coop.rchain.models.StacksafeMessage
import coop.rchain.models.either.implicits._
import coop.rchain.shared._
import coop.rchain.metrics.Span
import coop.rchain.metrics.Span.TraceId
import monix.eval.Task
import monix.execution.Scheduler
import monix.reactive.Observable

private[api] object DeployGrpcService {
  def instance[F[_]: Concurrent: Log: SafetyOracle: BlockStore: Taskable: Span: EngineCell](
      blockApiLock: Semaphore[F],
      tracing: Boolean
  )(
      implicit worker: Scheduler
  ): DeployServiceGrpcMonix.DeployService =
    new DeployServiceGrpcMonix.DeployService {

      private def defer[A <: StacksafeMessage[A]](
          task: F[Either[String, A]]
      ): Task[GrpcEither] =
        Task
          .defer(task.toTask)
          .executeOn(worker)
          .executeWithOptions(TaskContrib.enableTracing(tracing))
          .attemptAndLog
          .attempt
          .map(_.fold(_.asLeft[A].toGrpcEither, _.toGrpcEither))

      private def deferList[A <: StacksafeMessage[A]](task: F[List[A]]): Task[List[GrpcEither]] =
        Task
          .defer(task.toTask)
          .executeOn(worker)
          .executeWithOptions(TaskContrib.enableTracing(tracing))
          .attemptAndLog
          .attempt
          .map(_.fold(t => List(t.asLeft[A].toGrpcEither), _.map(_.asRight[String].toGrpcEither)))

      override def doDeploy(d: DeployData): Task[GrpcEither] =
        defer(BlockAPI.deploy[F](d, Span.next))

      override def getBlock(q: BlockQuery): Task[GrpcEither] =
        defer(BlockAPI.getBlock[F](q, Span.next))

      override def visualizeDag(q: VisualizeDagQuery): Observable[GrpcEither] = {
        type Effect[A] = StateT[Id, Vector[String], A]
        implicit val ser: GraphSerializer[Effect] = new ListSerializer[Effect]
        val serialize: Effect[Graphz[Effect]] => List[VisualizeBlocksResponse] =
          _.runS(Vector.empty).toList.map(VisualizeBlocksResponse(_))

        val depth  = if (q.depth <= 0) None else Some(q.depth)
        val config = GraphConfig(q.showJustificationLines)

        Observable
          .fromTask(
            deferList[VisualizeBlocksResponse](
              Functor[F].map {
                implicit val traceId: TraceId = Span.next
                BlockAPI.visualizeDag[F, Effect, List[VisualizeBlocksResponse]](
                  depth,
                  (ts, lfb) => GraphzGenerator.dagAsCluster[F, Effect](ts, lfb, config),
                  serialize
                )
              }(_.getOrElse(List.empty[VisualizeBlocksResponse]))
            )
          )
          .flatMap(Observable.fromIterable)
      }

      override def machineVerifiableDag(q: MachineVerifyQuery): Task[GrpcEither] =
        defer(BlockAPI.machineVerifiableDag[F])

      override def getBlocks(request: BlocksQuery): Observable[GrpcEither] =
        Observable
          .fromTask(
            deferList[LightBlockInfo] {
              implicit val traceId: TraceId = Span.next
              Functor[F].map(BlockAPI.getBlocks[F](Some(request.depth)))(
                _.getOrElse(List.empty[LightBlockInfo])
              )
            }
          )
          .flatMap(Observable.fromIterable)

      override def listenForDataAtName(request: DataAtNameQuery): Task[GrpcEither] =
        defer {
          implicit val traceId: Span.TraceId = Span.next
          BlockAPI.getListeningNameDataResponse[F](request.depth, request.name.get)
        }

      override def listenForContinuationAtName(
          request: ContinuationAtNameQuery
      ): Task[GrpcEither] =
        defer {
          implicit val traceId: Span.TraceId = Span.next
          BlockAPI.getListeningNameContinuationResponse[F](request.depth, request.names)
        }

      override def showMainChain(request: BlocksQuery): Observable[GrpcEither] =
        Observable
          .fromTask {
            implicit val traceId: TraceId = Span.next
            deferList(BlockAPI.showMainChain[F](request.depth))
          }
          .flatMap(Observable.fromIterable)

      override def findBlockWithDeploy(request: FindDeployInBlockQuery): Task[GrpcEither] =
        defer {
          implicit val traceId: TraceId = Span.next
          BlockAPI.findBlockWithDeploy[F](request.user, request.timestamp)
        }

      override def findDeploy(request: FindDeployQuery): Task[GrpcEither] =
        defer {
          implicit val traceId: TraceId = Span.next
          BlockAPI.findDeploy[F](request.deployId.toByteArray)
        }

      override def previewPrivateNames(
          request: PrivateNamePreviewQuery
      ): Task[GrpcEither] =
        defer(BlockAPI.previewPrivateNames[F](request.user, request.timestamp, request.nameQty))

      override def lastFinalizedBlock(request: LastFinalizedBlockQuery): Task[GrpcEither] =
        defer {
          implicit val traceId: TraceId = Span.next
          BlockAPI.lastFinalizedBlock[F]()
        }
    }
}
