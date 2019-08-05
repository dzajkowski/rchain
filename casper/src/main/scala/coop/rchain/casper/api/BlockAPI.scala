package coop.rchain.casper.api

import cats.Monad
import cats.effect.concurrent.Semaphore
import cats.effect.{Concurrent, Sync}
import cats.implicits._
import com.google.protobuf.ByteString
import coop.rchain.blockstorage.{BlockDagRepresentation, BlockStore}
import coop.rchain.casper.engine.{EngineCell, _}
import EngineCell._
import coop.rchain.casper.DeployError._
import coop.rchain.casper.MultiParentCasper.ignoreDoppelgangerCheck
import coop.rchain.casper._
import coop.rchain.casper.protocol._
import coop.rchain.casper.util.rholang.RuntimeManager
import coop.rchain.casper.util.{EventConverter, ProtoUtil}
import coop.rchain.crypto.codec.Base16
import coop.rchain.crypto.hash.Blake2b512Random
import coop.rchain.graphz._
import coop.rchain.metrics.Span.TraceId
import coop.rchain.metrics.{Metrics, Span}
import coop.rchain.models.BlockHash.BlockHash
import coop.rchain.models.Par
import coop.rchain.models.rholang.sorter.Sortable._
import coop.rchain.models.serialization.implicits.mkProtobufInstance
import coop.rchain.rholang.interpreter.storage.StoragePrinter
import coop.rchain.rspace.StableHashProvider
import coop.rchain.rspace.trace._
import coop.rchain.shared.Log

import scala.collection.immutable

object BlockAPI {

  type Error           = String
  type ApiErr[A]       = Either[Error, A]
  type Effect[F[_], A] = F[ApiErr[A]]

  val BlockAPIMetricsSource: Metrics.Source = Metrics.Source(Metrics.BaseSource, "block-api")
  val CreateBlockSource: Metrics.Source     = Metrics.Source(BlockAPIMetricsSource, "create-block")
  val DeploySource: Metrics.Source          = Metrics.Source(BlockAPIMetricsSource, "deploy")
  val GetBlockSource: Metrics.Source        = Metrics.Source(BlockAPIMetricsSource, "get-block")

  def deploy[F[_]: Monad: EngineCell: Log: Span](
      d: DeployData,
      parentTraceId: TraceId
  ): Effect[F, DeployServiceResponse] = Span[F].trace(DeploySource, parentTraceId) { _ =>
    def casperDeploy(casper: MultiParentCasper[F]): Effect[F, DeployServiceResponse] =
      casper
        .deploy(d)
        .map(
          _.bimap(
            err => err.show,
            res =>
              DeployServiceResponse(
                s"Success!\nDeployId is: ${PrettyPrinter.buildStringNoLimit(res)}"
              )
          )
        )

    val errorMessage = "Could not deploy, casper instance was not available yet."

    EngineCell[F].read >>= (_.withCasper[ApiErr[DeployServiceResponse]](
      casperDeploy,
      Log[F].warn(errorMessage).as(s"Error: $errorMessage".asLeft)
    ))
  }

  def createBlock[F[_]: Sync: Concurrent: EngineCell: Log: Span](
      blockApiLock: Semaphore[F],
      parentTraceId: TraceId,
      printUnmatchedSends: Boolean = false
  ): Effect[F, DeployServiceResponse] = Span[F].trace(CreateBlockSource, parentTraceId) {
    implicit traceId =>
      val errorMessage = "Could not create block, casper instance was not available yet."
      EngineCell[F].read >>= (
        _.withCasper[ApiErr[DeployServiceResponse]](
          casper => {
            Sync[F].bracket(blockApiLock.tryAcquire) {
              case true =>
                for {
                  maybeBlock <- casper.createBlock
                  result <- maybeBlock match {
                             case err: NoBlock =>
                               s"Error while creating block: $err"
                                 .asLeft[DeployServiceResponse]
                                 .pure[F]
                             case Created(block) =>
                               casper
                                 .addBlock(block, ignoreDoppelgangerCheck[F]) >>= (addResponse(
                                 _,
                                 block,
                                 casper,
                                 printUnmatchedSends
                               ))
                           }
                } yield result
              case false =>
                "Error: There is another propose in progress.".asLeft[DeployServiceResponse].pure[F]
            } {
              case true =>
                blockApiLock.release
              case false =>
                ().pure[F]
            }
          },
          default = Log[F]
            .warn(errorMessage)
            .as(s"Error: $errorMessage".asLeft)
        )
      )
  }

  def getListeningNameDataResponse[F[_]: Concurrent: EngineCell: Log: SafetyOracle: BlockStore](
      depth: Int,
      listeningName: Par
  )(implicit traceId: TraceId): Effect[F, ListeningNameDataResponse] = {

    val errorMessage = "Could not get listening name data, casper instance was not available yet."

    def casperResponse(
        implicit casper: MultiParentCasper[F],
        traceId: TraceId
    ): Effect[F, ListeningNameDataResponse] =
      for {
        mainChain           <- getMainChainFromTip[F](depth)
        runtimeManager      <- casper.getRuntimeManager
        sortedListeningName <- parSortable.sortMatch[F](listeningName).map(_.term)
        maybeBlocksWithActiveName <- mainChain.toList.traverse { block =>
                                      getDataWithBlockInfo[F](
                                        runtimeManager,
                                        sortedListeningName,
                                        block
                                      )
                                    }
        blocksWithActiveName = maybeBlocksWithActiveName.flatten
      } yield ListeningNameDataResponse(
        blockResults = blocksWithActiveName,
        length = blocksWithActiveName.length
      ).asRight

    EngineCell[F].read >>= (_.withCasper[ApiErr[ListeningNameDataResponse]](
      casper => casperResponse(casper, traceId),
      Log[F]
        .warn(errorMessage)
        .as(s"Error: $errorMessage".asLeft)
    ))
  }

  def getListeningNameContinuationResponse[F[_]: Concurrent: EngineCell: Log: SafetyOracle: BlockStore](
      depth: Int,
      listeningNames: Seq[Par]
  )(implicit traceId: TraceId): Effect[F, ListeningNameContinuationResponse] = {
    val errorMessage =
      "Could not get listening names continuation, casper instance was not available yet."
    def casperResponse(
        implicit casper: MultiParentCasper[F]
    ): Effect[F, ListeningNameContinuationResponse] =
      for {
        mainChain      <- getMainChainFromTip[F](depth)
        runtimeManager <- casper.getRuntimeManager
        sortedListeningNames <- listeningNames.toList
                                 .traverse(parSortable.sortMatch[F](_).map(_.term))
        maybeBlocksWithActiveName <- mainChain.toList.traverse { block =>
                                      getContinuationsWithBlockInfo[F](
                                        runtimeManager,
                                        sortedListeningNames,
                                        block
                                      )
                                    }
        blocksWithActiveName = maybeBlocksWithActiveName.flatten
      } yield ListeningNameContinuationResponse(
        blockResults = blocksWithActiveName,
        length = blocksWithActiveName.length
      ).asRight

    EngineCell[F].read >>= (_.withCasper[ApiErr[ListeningNameContinuationResponse]](
      casperResponse(_),
      Log[F]
        .warn(errorMessage)
        .as(s"Error: $errorMessage".asLeft)
    ))
  }

  private def getMainChainFromTip[F[_]: Monad: Log: SafetyOracle: BlockStore](depth: Int)(
      implicit casper: MultiParentCasper[F],
      traceId: TraceId
  ): F[IndexedSeq[BlockMessage]] =
    for {
      dag       <- casper.blockDag
      tipHashes <- casper.estimator(dag)
      tipHash   = tipHashes.head
      tip       <- ProtoUtil.unsafeGetBlock[F](tipHash)
      mainChain <- ProtoUtil.getMainChainUntilDepth[F](tip, IndexedSeq.empty[BlockMessage], depth)
    } yield mainChain

  private def getDataWithBlockInfo[F[_]: Log: SafetyOracle: BlockStore: Concurrent](
      runtimeManager: RuntimeManager[F],
      sortedListeningName: Par,
      block: BlockMessage
  )(implicit casper: MultiParentCasper[F], traceId: TraceId): F[Option[DataWithBlockInfo]] =
    if (isListeningNameReduced(block, immutable.Seq(sortedListeningName))) {
      val stateHash =
        ProtoUtil.tuplespace(block).get
      for {
        data      <- runtimeManager.getData(stateHash)(sortedListeningName)
        blockInfo <- getLightBlockInfo[F](block)
      } yield Option[DataWithBlockInfo](DataWithBlockInfo(data, Some(blockInfo)))
    } else {
      none[DataWithBlockInfo].pure[F]
    }

  private def getContinuationsWithBlockInfo[F[_]: Log: SafetyOracle: BlockStore: Concurrent](
      runtimeManager: RuntimeManager[F],
      sortedListeningNames: immutable.Seq[Par],
      block: BlockMessage
  )(
      implicit casper: MultiParentCasper[F],
      traceId: TraceId
  ): F[Option[ContinuationsWithBlockInfo]] =
    if (isListeningNameReduced(block, sortedListeningNames)) {
      val stateHash =
        ProtoUtil.tuplespace(block).get
      for {
        continuations <- runtimeManager.getContinuation(stateHash)(sortedListeningNames)
        continuationInfos = continuations.map(
          continuation => WaitingContinuationInfo(continuation._1, Some(continuation._2))
        )
        blockInfo <- getLightBlockInfo[F](block)
      } yield Option[ContinuationsWithBlockInfo](
        ContinuationsWithBlockInfo(continuationInfos, Some(blockInfo))
      )
    } else {
      none[ContinuationsWithBlockInfo].pure[F]
    }

  private def isListeningNameReduced(
      block: BlockMessage,
      sortedListeningName: immutable.Seq[Par]
  ): Boolean = {
    val serializedLog = for {
      bd    <- block.body.toSeq
      pd    <- bd.deploys
      event <- pd.deployLog
    } yield event
    val log =
      serializedLog.map(EventConverter.toRspaceEvent)
    log.exists {
      case Produce(channelHash, _, _, _) =>
        channelHash == StableHashProvider.hash(sortedListeningName)
      case Consume(channelsHashes, _, _, _) =>
        channelsHashes.toList.sorted == sortedListeningName
          .map(StableHashProvider.hash(_))
          .toList
          .sorted
      case COMM(consume, produces, _) =>
        (consume.channelsHashes.toList.sorted ==
          sortedListeningName.map(StableHashProvider.hash(_)).toList.sorted) ||
          produces.exists(
            produce => produce.channelsHash == StableHashProvider.hash(sortedListeningName)
          )
    }
  }

  private def toposortDag[
      F[_]: Monad: EngineCell: Log: SafetyOracle: BlockStore,
      A
  ](maybeDepth: Option[Int])(
      doIt: (MultiParentCasper[F], Vector[Vector[BlockHash]]) => F[ApiErr[A]]
  ): Effect[F, A] = {

    val errorMessage =
      "Could not visualize graph, casper instance was not available yet."

    def casperResponse(implicit casper: MultiParentCasper[F]): Effect[F, A] =
      for {
        dag      <- MultiParentCasper[F].blockDag
        depth    <- maybeDepth.fold(dag.topoSort(0L).map(_.length - 1))(_.pure[F])
        topoSort <- dag.topoSortTail(depth)
        result   <- doIt(casper, topoSort)
      } yield result

    EngineCell[F].read >>= (_.withCasper[ApiErr[A]](
      casperResponse(_),
      Log[F].warn(errorMessage).as(errorMessage.asLeft)
    ))
  }

  def visualizeDag[
      F[_]: Monad: Sync: EngineCell: Log: SafetyOracle: BlockStore,
      G[_]: Monad: GraphSerializer,
      R
  ](
      depth: Option[Int],
      visualizer: (Vector[Vector[BlockHash]], String) => F[G[Graphz[G]]],
      serialize: G[Graphz[G]] => R
  )(implicit traceId: TraceId): Effect[F, R] =
    toposortDag[F, R](depth) {
      case (casper, topoSort) =>
        for {
          lfb   <- casper.lastFinalizedBlock
          graph <- visualizer(topoSort, PrettyPrinter.buildString(lfb.blockHash))
        } yield serialize(graph).asRight[Error]
    }

  def machineVerifiableDag[
      F[_]: Monad: Sync: EngineCell: Log: SafetyOracle: BlockStore
  ]: Effect[F, MachineVerifyResponse] =
    toposortDag[F, MachineVerifyResponse](maybeDepth = None) {
      case (_, topoSort) =>
        val fetchParents: BlockHash => F[List[BlockHash]] = { blockHash =>
          ProtoUtil.unsafeGetBlock[F](blockHash) map (_.getHeader.parentsHashList.toList)
        }

        MachineVerifiableDag[F](topoSort, fetchParents)
          .map(_.map(edges => edges.show).mkString("\n"))
          .map(MachineVerifyResponse(_).asRight[Error])
    }

  def getBlocks[F[_]: Monad: EngineCell: Log: SafetyOracle: BlockStore](
      depth: Option[Int]
  )(implicit traceId: TraceId): Effect[F, List[LightBlockInfo]] =
    toposortDag[F, List[LightBlockInfo]](depth) {
      case (casper, topoSort) =>
        implicit val ev: MultiParentCasper[F] = casper
        topoSort
          .foldM(List.empty[LightBlockInfo]) {
            case (blockInfosAtHeightAcc, blockHashesAtHeight) =>
              for {
                blocksAtHeight     <- blockHashesAtHeight.traverse(ProtoUtil.unsafeGetBlock[F])
                blockInfosAtHeight <- blocksAtHeight.traverse(b => getLightBlockInfo[F](b))
              } yield blockInfosAtHeightAcc ++ blockInfosAtHeight
          }
          .map(_.reverse.asRight[Error])
    }

  def showMainChain[F[_]: Monad: EngineCell: Log: SafetyOracle: BlockStore](
      depth: Int
  )(implicit traceId: TraceId): F[List[LightBlockInfo]] = {

    val errorMessage =
      "Could not show main chain, casper instance was not available yet."

    def casperResponse(implicit casper: MultiParentCasper[F]) =
      for {
        dag        <- MultiParentCasper[F].blockDag
        tipHashes  <- MultiParentCasper[F].estimator(dag)
        tipHash    = tipHashes.head
        tip        <- ProtoUtil.unsafeGetBlock[F](tipHash)
        mainChain  <- ProtoUtil.getMainChainUntilDepth[F](tip, IndexedSeq.empty[BlockMessage], depth)
        blockInfos <- mainChain.toList.traverse(b => getLightBlockInfo[F](b))
      } yield blockInfos

    EngineCell[F].read >>= (_.withCasper[List[LightBlockInfo]](
      casperResponse(_),
      Log[F].warn(errorMessage).as(List.empty[LightBlockInfo])
    ))
  }

  def findDeploy[F[_]: Monad: EngineCell: Log: SafetyOracle: BlockStore](
      id: DeployId
  )(implicit traceId: TraceId): Effect[F, LightBlockQueryResponse] =
    EngineCell[F].read >>= (
      _.withCasper[ApiErr[LightBlockQueryResponse]](
        implicit casper =>
          for {
            dag               <- casper.blockDag
            allBlocksTopoSort <- dag.topoSort(0L)
            maybeBlock <- allBlocksTopoSort.flatten.reverse.toStream
                           .traverse(ProtoUtil.unsafeGetBlock[F])
                           .map(_.find(ProtoUtil.containsDeploy(_, ByteString.copyFrom(id))))
            response <- maybeBlock.traverse(b => getLightBlockInfo[F](b))
          } yield response.fold(
            s"Couldn't find block containing deploy with id: ${PrettyPrinter
              .buildStringNoLimit(id)}".asLeft[LightBlockQueryResponse]
          )(
            blockInfo =>
              LightBlockQueryResponse(
                blockInfo = Some(blockInfo)
              ).asRight
          ),
        Log[F]
          .warn("Could not find block with deploy, casper instance was not available yet.")
          .as(s"Error: errorMessage".asLeft)
      )
    )

  // TODO: Replace with call to BlockStore
  def findBlockWithDeploy[F[_]: Monad: EngineCell: Log: SafetyOracle: BlockStore](
      user: ByteString,
      timestamp: Long
  )(implicit traceId: TraceId): Effect[F, BlockQueryResponse] = {

    val errorMessage =
      "Could not find block with deploy, casper instance was not available yet."

    def casperResponse(
        implicit casper: MultiParentCasper[F]
    ): Effect[F, BlockQueryResponse] =
      for {
        dag                <- MultiParentCasper[F].blockDag
        allBlocksTopoSort  <- dag.topoSort(0L)
        maybeBlock         <- findBlockWithDeploy[F](allBlocksTopoSort.flatten.reverse, user, timestamp)
        blockQueryResponse <- maybeBlock.traverse(b => getFullBlockInfo[F](b))
      } yield blockQueryResponse.fold(
        s"Error: Failure to find block containing deploy signed by ${PrettyPrinter
          .buildString(user)} with timestamp ${timestamp.toString}".asLeft[BlockQueryResponse]
      )(
        blockInfo =>
          BlockQueryResponse(
            blockInfo = Some(blockInfo)
          ).asRight
      )

    EngineCell[F].read >>= (_.withCasper[ApiErr[BlockQueryResponse]](
      casperResponse(_),
      Log[F]
        .warn(errorMessage)
        .as(s"Error: errorMessage".asLeft)
    ))
  }

  private def findBlockWithDeploy[F[_]: Monad: Log: BlockStore](
      blockHashes: Vector[BlockHash],
      user: ByteString,
      timestamp: Long
  ): F[Option[BlockMessage]] =
    blockHashes.toStream
      .traverse(ProtoUtil.unsafeGetBlock[F])
      .map(blocks => blocks.find(ProtoUtil.containsDeploy(_, user, timestamp)))

  def getBlock[F[_]: Monad: EngineCell: Log: SafetyOracle: BlockStore: Span](
      q: BlockQuery,
      parentTraceId: TraceId
  ): Effect[F, BlockQueryResponse] = Span[F].trace(GetBlockSource, parentTraceId) {
    implicit traceId =>
      val errorMessage =
        "Could not get block, casper instance was not available yet."

      def casperResponse(
          implicit casper: MultiParentCasper[F]
      ): Effect[F, BlockQueryResponse] =
        for {
          dag        <- MultiParentCasper[F].blockDag
          maybeBlock <- getBlock[F](q, dag)
          blockQueryResponse <- maybeBlock match {
                                 case Some(block) =>
                                   for {
                                     blockInfo <- getFullBlockInfo[F](block)
                                   } yield BlockQueryResponse(blockInfo = Some(blockInfo)).asRight
                                 case None =>
                                   s"Error: Failure to find block with hash ${q.hash}".asLeft
                                     .pure[F]
                               }
        } yield blockQueryResponse

      EngineCell[F].read >>= (_.withCasper[ApiErr[BlockQueryResponse]](
        casperResponse(_),
        Log[F].warn(errorMessage).as(s"Error: $errorMessage".asLeft)
      ))
  }

  private def getBlockInfo[A, F[_]: Monad: SafetyOracle: BlockStore](
      block: BlockMessage,
      constructor: (
          BlockMessage,
          Long,
          Int,
          BlockHash,
          Long,
          BlockHash,
          Seq[BlockHash],
          Float,
          Float,
          Seq[Bond],
          Seq[ProcessedDeploy]
      ) => F[A],
      traceId: TraceId
  )(implicit casper: MultiParentCasper[F]): F[A] =
    for {
      dag         <- casper.blockDag
      header      = block.header.getOrElse(Header.defaultInstance)
      version     = header.version
      deployCount = header.deployCount
      tsHash = ProtoUtil.tuplespace(block) match {
        case Some(hash) => hash
        case None       => ByteString.EMPTY
      }
      timestamp       = header.timestamp
      mainParent      = header.parentsHashList.headOption.getOrElse(ByteString.EMPTY)
      parentsHashList = header.parentsHashList
      normalizedFaultTolerance <- SafetyOracle[F]
                                   .normalizedFaultTolerance(dag, block.blockHash)(traceId) // TODO: Warn about parent block finalization
      initialFault       <- casper.normalizedInitialFault(ProtoUtil.weightMap(block))
      bondsValidatorList = ProtoUtil.bonds(block)
      processedDeploy    = ProtoUtil.deploys(block)
      blockInfo <- constructor(
                    block,
                    version,
                    deployCount,
                    tsHash,
                    timestamp,
                    mainParent,
                    parentsHashList,
                    normalizedFaultTolerance,
                    initialFault,
                    bondsValidatorList,
                    processedDeploy
                  )
    } yield blockInfo

  private def getFullBlockInfo[F[_]: Monad: SafetyOracle: BlockStore](
      block: BlockMessage
  )(implicit casper: MultiParentCasper[F], traceId: TraceId): F[BlockInfo] =
    getBlockInfo[BlockInfo, F](block, constructBlockInfo[F], traceId)
  private def getLightBlockInfo[F[_]: Monad: SafetyOracle: BlockStore](
      block: BlockMessage
  )(implicit casper: MultiParentCasper[F], traceId: TraceId): F[LightBlockInfo] =
    getBlockInfo[LightBlockInfo, F](block, constructLightBlockInfo[F], traceId)

  private def constructBlockInfo[F[_]: Monad: SafetyOracle: BlockStore](
      block: BlockMessage,
      version: Long,
      deployCount: Int,
      tsHash: BlockHash,
      timestamp: Long,
      mainParent: BlockHash,
      parentsHashList: Seq[BlockHash],
      normalizedFaultTolerance: Float,
      initialFault: Float,
      bondsValidatorList: Seq[Bond],
      processedDeploys: Seq[ProcessedDeploy]
  ): F[BlockInfo] =
    BlockInfo(
      blockHash = PrettyPrinter.buildStringNoLimit(block.blockHash),
      blockSize = block.serializedSize.toString,
      blockNumber = ProtoUtil.blockNumber(block),
      version = version,
      deployCount = deployCount,
      tupleSpaceHash = PrettyPrinter.buildStringNoLimit(tsHash),
      timestamp = timestamp,
      faultTolerance = normalizedFaultTolerance - initialFault, // TODO: Fix
      mainParentHash = PrettyPrinter.buildStringNoLimit(mainParent),
      parentsHashList = parentsHashList.map(PrettyPrinter.buildStringNoLimit),
      sender = PrettyPrinter.buildStringNoLimit(block.sender),
      shardId = block.shardId,
      bondsValidatorList = bondsValidatorList.map(PrettyPrinter.buildString),
      deployCost = processedDeploys.map(PrettyPrinter.buildString)
    ).pure[F]

  private def constructLightBlockInfo[F[_]: Monad: SafetyOracle: BlockStore](
      block: BlockMessage,
      version: Long,
      deployCount: Int,
      tsHash: BlockHash,
      timestamp: Long,
      mainParent: BlockHash,
      parentsHashList: Seq[BlockHash],
      normalizedFaultTolerance: Float,
      initialFault: Float,
      bondsValidatorList: Seq[Bond],
      processedDeploys: Seq[ProcessedDeploy]
  ): F[LightBlockInfo] =
    LightBlockInfo(
      blockHash = PrettyPrinter.buildStringNoLimit(block.blockHash),
      blockSize = block.serializedSize.toString,
      blockNumber = ProtoUtil.blockNumber(block),
      version = version,
      deployCount = deployCount,
      tupleSpaceHash = PrettyPrinter.buildStringNoLimit(tsHash),
      timestamp = timestamp,
      faultTolerance = normalizedFaultTolerance - initialFault,
      mainParentHash = PrettyPrinter.buildStringNoLimit(mainParent),
      parentsHashList = parentsHashList.map(PrettyPrinter.buildStringNoLimit),
      sender = PrettyPrinter.buildStringNoLimit(block.sender)
    ).pure[F]

  private def getBlock[F[_]: Monad: BlockStore](
      q: BlockQuery,
      dag: BlockDagRepresentation[F]
  ): F[Option[BlockMessage]] =
    for {
      findResult <- BlockStore[F].find(h => Base16.encode(h.toByteArray).startsWith(q.hash))
    } yield findResult.headOption match {
      case Some((_, block)) => Some(block)
      case None             => none[BlockMessage]
    }

  private def addResponse[F[_]: Concurrent](
      status: BlockStatus,
      block: BlockMessage,
      casper: MultiParentCasper[F],
      printUnmatchedSends: Boolean
  ): Effect[F, DeployServiceResponse] =
    status match {
      case _: InvalidBlock =>
        s"Failure! Invalid block: $status".asLeft[DeployServiceResponse].pure[F]
      case _: ValidBlock =>
        val hash    = PrettyPrinter.buildString(block.blockHash)
        val deploys = block.body.get.deploys.map(_.deploy.get)
        val maybeUnmatchedSendsOutputF =
          if (printUnmatchedSends) prettyPrintUnmatchedSends(casper, deploys).map(_.some)
          else none[String].pure[F]
        maybeUnmatchedSendsOutputF >>= (
            maybeOutput =>
              DeployServiceResponse(
                s"Success! Block $hash created and added.${maybeOutput.map("\n" + _).getOrElse("")}"
              ).asRight[Error].pure[F]
          )
      case BlockException(ex) =>
        s"Error during block processing: $ex".asLeft[DeployServiceResponse].pure[F]
      case Processing =>
        "No action taken since other thread is already processing the block."
          .asLeft[DeployServiceResponse]
          .pure[F]
    }

  private def prettyPrintUnmatchedSends[F[_]: Concurrent](
      casper: MultiParentCasper[F],
      deploys: Seq[DeployData]
  ): F[String] =
    casper.getRuntimeManager >>= (
      _.withRuntimeLock(runtime => StoragePrinter.prettyPrintUnmatchedSends(deploys, runtime))
    )

  def previewPrivateNames[F[_]: Monad: Log](
      deployer: ByteString,
      timestamp: Long,
      nameQty: Int
  ): Effect[F, PrivateNamePreviewResponse] = {
    val seed    = DeployData().withDeployer(deployer).withTimestamp(timestamp)
    val rand    = Blake2b512Random(DeployData.toByteArray(seed))
    val safeQty = nameQty min 1024
    val ids     = (0 until safeQty).map(_ => ByteString.copyFrom(rand.next()))
    PrivateNamePreviewResponse(ids).asRight[String].pure[F]
  }

  def lastFinalizedBlock[F[_]: Monad: EngineCell: SafetyOracle: BlockStore: Log]()(
      implicit traceId: TraceId
  ): Effect[F, LastFinalizedBlockResponse] = {
    val errorMessage = "Could not get last finalized block, casper instance was not available yet."
    EngineCell[F].read >>= (
      _.withCasper[ApiErr[LastFinalizedBlockResponse]](
        implicit casper =>
          for {
            lastFinalizedBlock <- casper.lastFinalizedBlock
            blockInfo          <- getFullBlockInfo[F](lastFinalizedBlock)
          } yield LastFinalizedBlockResponse(blockInfo = Some(blockInfo)).asRight,
        Log[F].warn(errorMessage).as(s"Error: $errorMessage".asLeft)
      )
    )
  }
}
