package coop.rchain.rholang

import coop.rchain.crypto.hash.Blake2b512Random
import coop.rchain.models
import coop.rchain.models.TaggedContinuation.TaggedCont.ParBody
import coop.rchain.models.serialization.implicits.mkProtobufInstance
import coop.rchain.models.{
  BindPattern,
  ListParWithRandom,
  Par,
  ParWithRandom,
  ReceiveBind,
  TaggedContinuation
}
import coop.rchain.rholang.interpreter.ParBuilder
import coop.rchain.rholang.interpreter.storage.ChargingRSpaceTest.rand
import coop.rchain.rspace.Serialize
import coop.rchain.rspace.internal.{codecGNAT, GNAT, WaitingContinuation}
import monix.eval.Coeval
import org.scalatest.{FlatSpec, Matchers}
import coop.rchain.rholang.interpreter.storage.implicits._
import coop.rchain.rspace.trace.{Consume, Event}
import scodec.Codec
import coop.rchain.shared.AttemptOps._

class IWantToBelieve extends FlatSpec with Matchers {
  behavior of "Rholang terms RSpace serialization"

  //                  Par,
  //                  BindPattern,

  implicit val codecBP    = serializeBindPattern.toCodec
  implicit val codecPar   = serializePar.toCodec
  implicit val codecLPR   = serializePars.toCodec
  implicit val codecTC    = serializeTaggedContinuation.toCodec
  implicit val xcodecGNAT = codecGNAT(codecPar, codecBP, codecLPR, codecTC)

  it should "size consume" in {

    val rhoContinuation = s"""for(x <- @"test"){Nil}"""
    val ast             = ParBuilder[Coeval].buildNormalizedTerm(rhoContinuation).value()

    val random = Blake2b512Random(128)

    val receives = ast.receives
    val receive  = receives.head
    val continuation: TaggedContinuation =
      TaggedContinuation().withParBody(ParWithRandom(receive.body, random))

    val patterns: Seq[BindPattern] = receive.binds.map(rb => BindPattern(rb.patterns))
    val channels: Seq[Par]         = receive.binds.map(rb => { rb.source })
    println("chans " + channels)
    println("conts " + continuation)
    println("patts " + patterns)
    val source = Consume.create(channels, patterns, continuation, false)

    val d: WaitingContinuation[BindPattern, TaggedContinuation] =
      WaitingContinuation(patterns, continuation, false, source)

    val gnat: GNAT[Par, BindPattern, ListParWithRandom, TaggedContinuation] =
      GNAT(channels, Seq.empty, Seq(d))

    println("???? " + gnat)

    val x = Codec[GNAT[Par, BindPattern, ListParWithRandom, TaggedContinuation]].encode(gnat).get

    println(x.toByteVector.size)

  }

  it should "size produce" in {

    val rhoContinuation = s"""for(x <- @"test"){Nil}"""
    val ast             = ParBuilder[Coeval].buildNormalizedTerm(rhoContinuation).value()

    val receives = ast.receives
    val receive  = receives.head
    val continuation: TaggedContinuation =
      TaggedContinuation().withParBody(ParWithRandom(receive.body))

    val patterns: Seq[BindPattern] = receive.binds.map(rb => BindPattern(rb.patterns))
    val channels: Seq[Par]         = receive.binds.map(rb => { rb.source })
    println("chans " + channels)
    println("conts " + continuation)
    println("patts " + patterns)
    val source = Consume.create(channels, patterns, continuation, false)

    val d: WaitingContinuation[BindPattern, TaggedContinuation] =
      WaitingContinuation(patterns, continuation, false, source)

    val gnat: GNAT[Par, BindPattern, ListParWithRandom, TaggedContinuation] =
      GNAT(channels, Seq.empty, Seq(d))

    val x = Codec[GNAT[Par, BindPattern, ListParWithRandom, TaggedContinuation]].encode(gnat).get

    println(x.toByteVector.size)

  }

}
