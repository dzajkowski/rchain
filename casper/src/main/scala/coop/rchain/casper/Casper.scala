package coop.rchain.casper

import cats.effect.concurrent.Semaphore
import cats.effect.{Concurrent, Sync}
import cats.implicits._
import com.google.protobuf.ByteString
import coop.rchain.blockstorage.{BlockMetadata, BlockStore}
import coop.rchain.casper.Estimator.Validator
import coop.rchain.casper.protocol._
import coop.rchain.casper.util._
import coop.rchain.casper.util.rholang.RuntimeManager.StateHash
import coop.rchain.casper.util.rholang._
import coop.rchain.catscontrib._
import coop.rchain.comm.CommError.ErrorHandler
import coop.rchain.comm.rp.Connect.{ConnectionsCell, RPConfAsk}
import coop.rchain.comm.transport.TransportLayer
import coop.rchain.shared._
import monix.execution.Scheduler

trait Casper[F[_], A] {
  def addBlock(b: BlockMessage): F[BlockStatus]
  def contains(b: BlockMessage): F[Boolean]
  def deploy(d: DeployData): F[Either[Throwable, Unit]]
  def estimator(dag: BlockDag): F[A]
  def createBlock: F[CreateBlockStatus]
}

trait MultiParentCasper[F[_]] extends Casper[F, IndexedSeq[BlockMessage]] {
  def blockDag: F[BlockDag]
  def fetchDependencies: F[Unit]
  // This is the weight of faults that have been accumulated so far.
  // We want the clique oracle to give us a fault tolerance that is greater than
  // this initial fault weight combined with our fault tolerance threshold t.
  def normalizedInitialFault(weights: Map[Validator, Long]): F[Float]
  def lastFinalizedBlock: F[BlockMessage]
  def storageContents(hash: ByteString): F[String]
  // TODO: Refactor hashSetCasper to take a RuntimeManager[F] just like BlockStore[F]
  def getRuntimeManager: F[Option[RuntimeManager]]
}

object MultiParentCasper extends MultiParentCasperInstances {
  def apply[F[_]](implicit instance: MultiParentCasper[F]): MultiParentCasper[F] = instance
}

sealed abstract class MultiParentCasperInstances {

  def hashSetCasper[F[_]: Sync: Concurrent: Capture: ConnectionsCell: TransportLayer: Log: Time: ErrorHandler: SafetyOracle: BlockStore: RPConfAsk](
      runtimeManager: RuntimeManager,
      validatorId: Option[ValidatorIdentity],
      genesis: BlockMessage,
      shardId: String
  )(implicit scheduler: Scheduler): F[MultiParentCasper[F]] = {
    val genesisBonds          = ProtoUtil.bonds(genesis)
    val initialLatestMessages = genesisBonds.map(_.validator -> genesis).toMap
    val dag = BlockDag.empty
      .copy(
        latestMessages = initialLatestMessages,
        dataLookup = Map(genesis.blockHash -> BlockMetadata.fromBlock(genesis)),
        topoSort = Vector(Vector(genesis.blockHash))
      )
    for {
      maybePostGenesisStateHash <- InterpreterUtil
                                    .validateBlockCheckpoint[F](
                                      genesis,
                                      dag,
                                      runtimeManager
                                    )
      postGenesisStateHash <- maybePostGenesisStateHash match {
                               case Left(BlockException(ex)) => Sync[F].raiseError[StateHash](ex)
                               case Right(None) =>
                                 Sync[F].raiseError[StateHash](
                                   new Exception("Genesis tuplespace validation failed!")
                                 )
                               case Right(Some(hash)) => hash.pure[F]
                             }
      semaphore <- Semaphore[F](1)
    } yield
      new MultiParentCasperImpl[F](
        runtimeManager,
        validatorId,
        genesis,
        dag,
        postGenesisStateHash,
        shardId,
        semaphore
      )
  }
}
