/*
 * Copyright 2025 ThatScalaGuy
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package fs2.nats.protocol

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import fs2.{Chunk, Pipe, Stream}
import munit.ScalaCheckSuite
import org.scalacheck.{Arbitrary, Gen, Prop}
import org.scalacheck.Prop.forAll
import java.nio.charset.StandardCharsets

/** A/B equivalence gate for the flat-array `ProtocolParser` rewrite.
  *
  * Random sequences of valid NATS frames are serialized to wire bytes and fed
  * through both the new `ProtocolParser` and the frozen
  * [[ReferenceProtocolParser]] under many chunkings — including 1-byte chunks
  * (every byte offset is a split point: mid-CRLF, mid-payload, mid-header) —
  * and across a `ParserConfig` matrix. For every split the two parsers must
  * produce frame-identical output, or fail with the identical terminal error.
  * The reference parser is the v0.2.0 implementation; the existing
  * [[ProtocolParserSpec]] independently pins the reference to correct,
  * hand-checked expectations, so new ≡ reference proves new is correct for all
  * splits.
  */
class ProtocolParserPropSpec extends ScalaCheckSuite:

  override def scalaCheckTestParameters =
    super.scalaCheckTestParameters.withMinSuccessfulTests(200)

  // --- helpers -------------------------------------------------------------

  private def bytes(s: String): Array[Byte] =
    s.getBytes(StandardCharsets.UTF_8)

  private def concat(parts: List[Array[Byte]]): Array[Byte] =
    parts.foldLeft(Array.emptyByteArray)(_ ++ _)

  /** Rebuild payload chunks over canonical arrays so comparison never depends
    * on a particular `Chunk` backing representation.
    */
  private def norm(f: NatsFrame): NatsFrame = f match
    case m: NatsFrame.MsgFrame =>
      m.copy(payload = Chunk.array(m.payload.toArray))
    case h: NatsFrame.HMsgFrame =>
      h.copy(payload = Chunk.array(h.payload.toArray))
    case other => other

  private def run(
      parser: Pipe[IO, Byte, NatsFrame],
      chunks: List[Chunk[Byte]]
  ): Either[Throwable, List[NatsFrame]] =
    Stream
      .emits(chunks)
      .unchunks
      .through(parser)
      .compile
      .toList
      .attempt
      .unsafeRunSync()

  private def equiv(config: ParserConfig, chunks: List[Chunk[Byte]]): Prop =
    val a = run(ProtocolParser.parseStream[IO](config), chunks)
    val b = run(ReferenceProtocolParser.parseStream[IO](config), chunks)
    (a, b) match
      case (Right(x), Right(y)) =>
        Prop(x.map(norm) == y.map(norm)) :| s"new=$x\nref=$y"
      case (Left(e1), Left(e2)) =>
        Prop(
          e1.getClass == e2.getClass && e1.getMessage == e2.getMessage
        ) :| s"new-err=[${e1.getClass.getName}: ${e1.getMessage}]\nref-err=[${e2.getClass.getName}: ${e2.getMessage}]"
      case _ =>
        Prop(false) :| s"divergent terminal:\nnew=$a\nref=$b"

  // --- chunkings -----------------------------------------------------------

  private def single(wire: Array[Byte]): List[Chunk[Byte]] =
    if wire.isEmpty then Nil else List(Chunk.array(wire))

  private def oneByte(wire: Array[Byte]): List[Chunk[Byte]] =
    wire.iterator.map(b => Chunk.array(Array(b))).toList

  private def fixed(wire: Array[Byte], size: Int): List[Chunk[Byte]] =
    if wire.isEmpty then Nil
    else wire.grouped(size).map(a => Chunk.array(a)).toList

  private def bySizes(wire: Array[Byte], sizes: List[Int]): List[Chunk[Byte]] =
    if wire.isEmpty || sizes.isEmpty then single(wire)
    else
      val out = List.newBuilder[Chunk[Byte]]
      var i = 0
      var si = 0
      while i < wire.length do
        val s = math.max(1, sizes(si % sizes.length))
        val end = math.min(wire.length, i + s)
        out += Chunk.array(java.util.Arrays.copyOfRange(wire, i, end))
        i = end
        si += 1
      out.result()

  // --- generators ----------------------------------------------------------

  private val tokenChar: Gen[Char] =
    Gen.oneOf(('a' to 'z') ++ ('A' to 'Z') ++ ('0' to '9'))

  private def tokenOfLen(min: Int, max: Int): Gen[String] =
    for
      n <- Gen.choose(min, max)
      cs <- Gen.listOfN(n, tokenChar)
    yield cs.mkString

  private val subjectChar: Gen[Char] =
    Gen.oneOf(
      ('a' to 'z') ++ ('A' to 'Z') ++ ('0' to '9') ++ Seq('.', '_', '-')
    )

  private val genSubject: Gen[String] =
    for
      n <- Gen.choose(1, 12)
      cs <- Gen.listOfN(n, subjectChar)
    yield cs.mkString

  private val genSid: Gen[Long] = Gen.choose(0L, 1000000L)

  private val genPayload: Gen[Array[Byte]] =
    for
      n <- Gen.choose(0, 48)
      bs <- Gen.listOfN(n, Arbitrary.arbitrary[Byte])
    yield bs.toArray

  /** A valid NATS/1.0 header block: version/status line, optional header lines,
    * trailing blank line.
    */
  private val genHeaderBlock: Gen[Array[Byte]] =
    for
      statusLine <- Gen.oneOf(
        "NATS/1.0",
        "NATS/1.0 100 Idle Heartbeat",
        "NATS/1.0 503",
        "NATS/1.0 404 No Messages"
      )
      nh <- Gen.choose(0, 3)
      hs <- Gen.listOfN(
        nh,
        for
          k <- tokenOfLen(1, 8)
          v <- tokenOfLen(1, 8)
        yield s"$k: $v"
      )
    yield
      val sb = new StringBuilder
      sb.append(statusLine).append("\r\n")
      hs.foreach(h => sb.append(h).append("\r\n"))
      sb.append("\r\n")
      bytes(sb.toString)

  private val genMsgWire: Gen[Array[Byte]] =
    for
      subject <- genSubject
      sid <- genSid
      reply <- Gen.option(genSubject)
      payload <- genPayload
    yield
      val head = reply match
        case Some(r) => s"MSG $subject $sid $r ${payload.length}\r\n"
        case None    => s"MSG $subject $sid ${payload.length}\r\n"
      concat(List(bytes(head), payload, bytes("\r\n")))

  private val genHMsgWire: Gen[Array[Byte]] =
    for
      subject <- genSubject
      sid <- genSid
      reply <- Gen.option(genSubject)
      header <- genHeaderBlock
      payload <- genPayload
    yield
      val headerLen = header.length
      val totalLen = headerLen + payload.length
      val head = reply match
        case Some(r) => s"HMSG $subject $sid $r $headerLen $totalLen\r\n"
        case None    => s"HMSG $subject $sid $headerLen $totalLen\r\n"
      concat(List(bytes(head), header, payload, bytes("\r\n")))

  private val genSimpleWire: Gen[Array[Byte]] =
    Gen.oneOf(bytes("PING\r\n"), bytes("PONG\r\n"), bytes("+OK\r\n"))

  private val genErrWire: Gen[Array[Byte]] =
    for
      n <- Gen.choose(1, 4)
      words <- Gen.listOfN(n, tokenOfLen(1, 6))
    yield bytes(s"-ERR '${words.mkString(" ")}'\r\n")

  private val genInfoWire: Gen[Array[Byte]] =
    Gen.oneOf(
      bytes(
        """INFO {"server_id":"test","version":"2.9.0","proto":1,"go":"go1.19","host":"0.0.0.0","port":4222,"max_payload":1048576}""" + "\r\n"
      ),
      bytes(
        """INFO {"server_id":"s2","version":"2.10.1","proto":1,"go":"go1.21","host":"127.0.0.1","port":4222,"max_payload":65536,"headers":true}""" + "\r\n"
      )
    )

  private val genFrameWire: Gen[Array[Byte]] =
    Gen.frequency(
      (5, genMsgWire),
      (3, genHMsgWire),
      (2, genSimpleWire),
      (1, genErrWire),
      (1, genInfoWire)
    )

  private def genSequence(maxFrames: Int): Gen[Array[Byte]] =
    for
      n <- Gen.choose(0, maxFrames)
      frames <- Gen.listOfN(n, genFrameWire)
    yield concat(frames)

  private val genSizes: Gen[List[Int]] =
    Gen.nonEmptyListOf(Gen.choose(1, 17))

  private val genConfig: Gen[ParserConfig] =
    Gen.oneOf(
      ParserConfig.default,
      ParserConfig.default.copy(strictMode = false),
      ParserConfig.default.copy(strictCRLF = false),
      ParserConfig.default.copy(strictMode = false, strictCRLF = false),
      ParserConfig.default.copy(maxPayloadLimit = Some(1000000L)),
      ParserConfig.default.copy(maxControlLineLength = 64)
    )

  // --- properties ----------------------------------------------------------

  property("new ≡ reference across all input splits and configs") {
    forAll(genSequence(6), genSizes, genConfig) { (wire, sizes, config) =>
      Prop.all(
        equiv(config, single(wire)),
        equiv(config, oneByte(wire)),
        equiv(config, fixed(wire, 8192)),
        equiv(config, fixed(wire, 3)),
        equiv(config, bySizes(wire, sizes))
      )
    }
  }

  property("default config parses exactly one frame per serialized frame") {
    forAll(Gen.choose(1, 8).flatMap(Gen.listOfN(_, genFrameWire))) {
      framesList =>
        val wire = concat(framesList)
        run(
          ProtocolParser.parseStream[IO](ParserConfig.default),
          single(wire)
        ) match
          case Right(frames) =>
            Prop(frames.length == framesList.length) :|
              s"expected ${framesList.length} frames, got ${frames.length}"
          case Left(err) =>
            Prop(false) :| s"unexpected error: $err"
    }
  }

  property("new ≡ reference for large payloads (carry regrow path)") {
    val genLarge: Gen[Array[Byte]] =
      for
        subject <- genSubject
        sid <- genSid
        n <- Gen.choose(8000, 20000)
        bs <- Gen.listOfN(n, Arbitrary.arbitrary[Byte])
      yield
        val payload = bs.toArray
        concat(
          List(
            bytes(s"MSG $subject $sid ${payload.length}\r\n"),
            payload,
            bytes("\r\n")
          )
        )

    forAll(Gen.listOfN(3, genLarge), genConfig) { (frames, config) =>
      val wire = concat(frames)
      Prop.all(
        equiv(config, single(wire)),
        equiv(config, fixed(wire, 8192)),
        equiv(config, fixed(wire, 1000))
      )
    }
  }

  // --- targeted unit cases -------------------------------------------------

  test("payloads are copied out, not aliased into the reused carry") {
    // Two frames whose payloads occupy overlapping carry regions across reads.
    // If payloads were views into the carry (not copies), parsing the second
    // would corrupt the first after compaction.
    val wire = bytes("MSG a 1 3\r\nAAA\r\nMSG b 2 3\r\nBBB\r\n")
    run(
      ProtocolParser.parseStream[IO](ParserConfig.default),
      oneByte(wire)
    ) match
      case Right(List(m1: NatsFrame.MsgFrame, m2: NatsFrame.MsgFrame)) =>
        assertEquals(
          new String(m1.payload.toArray, StandardCharsets.UTF_8),
          "AAA"
        )
        assertEquals(
          new String(m2.payload.toArray, StandardCharsets.UTF_8),
          "BBB"
        )
      case other => fail(s"unexpected: $other")
  }

  test("a large single payload split across small chunks reassembles exactly") {
    val payload = Array.tabulate(50000)(i => (i % 256).toByte)
    val wire = concat(
      List(bytes(s"MSG big 1 ${payload.length}\r\n"), payload, bytes("\r\n"))
    )
    val chunks = fixed(wire, 137)
    run(ProtocolParser.parseStream[IO](ParserConfig.default), chunks) match
      case Right(List(m: NatsFrame.MsgFrame)) =>
        assert(java.util.Arrays.equals(m.payload.toArray, payload))
      case other => fail(s"unexpected: $other")
  }

  test("a no-CRLF run beyond maxControlLineLength fails (bounded, no hang)") {
    val config = ParserConfig.default.copy(maxControlLineLength = 16)
    val wire = Array.fill(200)('x'.toByte)
    val newR = run(ProtocolParser.parseStream[IO](config), oneByte(wire))
    val refR =
      run(ReferenceProtocolParser.parseStream[IO](config), oneByte(wire))
    assert(newR.isLeft, s"expected failure, got $newR")
    (newR, refR) match
      case (Left(e1), Left(e2)) =>
        assertEquals(e1.getClass, e2.getClass)
        assertEquals(e1.getMessage, e2.getMessage)
      case _ => fail(s"divergent: new=$newR ref=$refR")
  }

  test("empty input yields no frames") {
    assertEquals(
      run(ProtocolParser.parseStream[IO](ParserConfig.default), Nil),
      Right(List.empty[NatsFrame])
    )
  }
