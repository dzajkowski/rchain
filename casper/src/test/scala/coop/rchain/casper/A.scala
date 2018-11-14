package coop.rchain.casper
import cats.Id

import cats.{Applicative, ApplicativeError, Id, Monad, Traverse}
import cats.data.EitherT
import cats.effect.concurrent.Ref
import cats.effect.Sync
import cats.implicits._
import cats.effect.concurrent.Ref
import coop.rchain.casper.HashSetCasperTest.{buildGenesis, createBonds}
import coop.rchain.casper.genesis.contracts.{Faucet, PreWallet}
import coop.rchain.casper.helper.HashSetCasperTestNode
import coop.rchain.casper.helper.HashSetCasperTestNode.{appErrId, peerNode}
import coop.rchain.casper.util.comm.TransportLayerTestImpl
import coop.rchain.catscontrib.ApplicativeError_
import coop.rchain.comm.CommError
import coop.rchain.comm.protocol.routing.Protocol
import coop.rchain.comm.rp.Connect.Connections
import coop.rchain.crypto.codec.Base16
import coop.rchain.crypto.hash.Keccak256
import coop.rchain.crypto.signatures.{Ed25519, Secp256k1}
import coop.rchain.p2p.EffectsTestInstances.LogicalTime
import monix.execution.Scheduler

import scala.collection.mutable

object A {
  def a() = {
    implicit val s = Scheduler.fixedPool("three-threads", 3)
    implicit val sync = coop.rchain.catscontrib.effect.implicits.syncId
    implicit val cap = coop.rchain.catscontrib.idCapture

    val (otherSk, otherPk)          = Ed25519.newKeyPair
    val (validatorKeys, validators) = (1 to 4).map(_ => Ed25519.newKeyPair).unzip
    val (ethPivKeys, ethPubKeys)    = (1 to 4).map(_ => Secp256k1.newKeyPair).unzip
    val ethAddresses =
    ethPubKeys.map(pk => "0x" + Base16.encode(Keccak256.hash(pk.bytes.drop(1)).takeRight(20)))
    val wallets     = ethAddresses.map(addr => PreWallet(addr, BigInt(10001)))
    val bonds       = createBonds(validators)
    val minimumBond = 100L
    val genesis =
    buildGenesis(wallets, bonds, minimumBond, Long.MaxValue, Faucet.basicWalletFaucet, 0L)

    val errorHandler = ApplicativeError_.applicativeError[Id, CommError](appErrId)

    val storageSize: Long = 1024L * 1024 * 10
    implicit val timeEff = new LogicalTime[Id]
    val n     = validatorKeys.take(1).length
    val names = (1 to n).map(i => s"node-$i")
    val peers = names.map(peerNode(_, 40400))
    val msgQueues = peers
      .map(_ -> new mutable.Queue[Protocol]())
      .toMap
      .mapValues(Ref.unsafe[Id, mutable.Queue[Protocol]])
    val logicalTime: LogicalTime[Id] = new LogicalTime[Id]

    val sks = validatorKeys.take(1)

    val nodes =
      names
        .zip(peers)
        .zip(sks)
        .map {
          case ((n, p), sk) =>
            val tle = new TransportLayerTestImpl[Id](p, msgQueues)
            new HashSetCasperTestNode[Id](
              n,
              p,
              tle,
              genesis,
              sk,
              logicalTime,
              errorHandler,
              storageSize
            )
        }
        .toVector

    import Connections._
    //make sure all nodes know about each other
    val pairs = for {
      n <- nodes
      m <- nodes
      if n.local != m.local
    } yield (n, m)

    println(pairs)

    val r = for {
      res <- nodes.traverse { a =>
        println("will init" + a)
        a.initialize()
      }
    } yield res

    println(r)
    r.head
  }

  def main(args: Array[String]): Unit = {
    while (a().isRight) {
      print(".")
    }
  }

}
