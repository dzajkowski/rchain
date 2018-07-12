package coop.rchain.rspace.bench

import java.util.concurrent.TimeUnit

import coop.rchain.rspace.Blake2b256Hash
import org.openjdk.jmh.annotations._
import org.scalacheck.Arbitrary
import scodec.Codec
import coop.rchain.shared.AttemptOps._
import coop.rchain.shared.ByteVectorOps._

class KeyBench {
  import KeyBench._

  @Benchmark
  @BenchmarkMode(Array(Mode.Throughput))
  @OutputTimeUnit(TimeUnit.SECONDS)
  def prepareKeyUsingCodec(bs: KeyState): Unit =
    bs.hashes.foreach(Codec[Blake2b256Hash].encode(_).get.bytes.toDirectByteBuffer)

  @Benchmark
  @BenchmarkMode(Array(Mode.Throughput))
  @OutputTimeUnit(TimeUnit.SECONDS)
  def prepareKeyUsingRawBlakeHash(bs: KeyState): Unit =
    bs.hashes.foreach(_.bytes.toDirectByteBuffer)
}

object KeyBench {
  val arbitraryBlake2b256Hash: Arbitrary[Blake2b256Hash] =
    Arbitrary(Arbitrary.arbitrary[Array[Byte]].map(bytes => Blake2b256Hash.create(bytes)))

  @State(Scope.Benchmark)
  class KeyState {
    val hashes: List[Blake2b256Hash] =
      (for {
        _ <- 0 to 1000
      } yield arbitraryBlake2b256Hash.arbitrary.sample.get).toList
  }
}
