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

import cats.effect.Async
import com.github.plokhotnyuk.jsoniter_scala.core.*
import fs2.{Chunk, Pipe, Pull, Stream}
import java.nio.charset.StandardCharsets
import scala.util.{Failure, Success, Try}

/** Configuration for the NATS protocol parser.
  *
  * @param maxControlLineLength
  *   Maximum allowed length for control lines (default 4096)
  * @param maxPayloadLimit
  *   Optional safety cap on payload size (None = no limit)
  * @param strictCRLF
  *   Whether to require strict CRLF line endings (default true)
  * @param strictMode
  *   Whether to fail on incomplete final frames (true) or emit error frames
  *   (false)
  */
final case class ParserConfig(
    maxControlLineLength: Int = 4096,
    maxPayloadLimit: Option[Long] = None,
    strictCRLF: Boolean = true,
    strictMode: Boolean = true
)

object ParserConfig:
  val default: ParserConfig = ParserConfig()

/** Incremental NATS protocol parser.
  *
  * Converts a raw Stream[F, Byte] into a Stream[F, NatsFrame] following the
  * NATS protocol specification. Handles partial reads, CRLF framing, and exact
  * N-byte payload reads for MSG/HMSG/PUB/HPUB commands.
  *
  * Carry strategy: instead of accumulating unconsumed bytes as an immutable
  * `Chunk` (which turns into a `Chunk.Queue` after the first `++` and forces a
  * full `toArray` materialization on every control-line scan — O(n²) under
  * streaming load), the parser keeps a single growable `Array[Byte]` carry with
  * `(start, end)` offsets. Each upstream chunk is `arraycopy`-ed in exactly
  * once and all scanning runs over absolute array indices. The carry is owned
  * by the single frame-reader fiber that runs the pipe (allocated fresh per
  * materialization via `Stream.suspend`), and never escapes: every emitted
  * payload/header is copied out into a fresh immutable `Chunk`.
  */
object ProtocolParser:

  private val CR: Byte = '\r'.toByte
  private val LF: Byte = '\n'.toByte

  /** Initial carry capacity; matches the typical fs2-io `socket.reads` chunk
    * size so the common case never has to regrow.
    */
  private val InitialCapacity: Int = 8192

  /** JVM max array length headroom; growth never produces an array larger than
    * this (avoids `Int` overflow to a negative size).
    */
  private val MaxArray: Int = Int.MaxValue - 8

  /** Mutable parse carry, confined to a single fiber. `[start, end)` is the
    * live (not-yet-parsed) window into `carry`; bytes before `start` are
    * consumed and bytes at/after `end` are garbage.
    */
  private final class ParserState(initialCapacity: Int):
    var carry: Array[Byte] = new Array[Byte](initialCapacity)
    var start: Int = 0
    var end: Int = 0

  /** Outcome of attempting to parse one frame from the current carry window. */
  private sealed trait Step
  private object Step:
    /** A complete frame was parsed; `start` has already been advanced past it.
      */
    final case class Emit(frame: NatsFrame) extends Step

    /** Not enough bytes for a control line yet — pull more input. */
    case object NeedMoreControl extends Step

    /** Control line parsed; waiting for a `length`-byte payload (+ CRLF). */
    final case class NeedMorePayload(length: Int) extends Step

    /** Recoverable protocol error: raise in strict mode, else emit a
      * `ParseErrorFrame` and end the stream.
      */
    final case class FailSoft(message: String) extends Step

    /** Hard protocol error: always raise, regardless of strict mode. */
    final case class FailHard(message: String) extends Step

  /** Create a parsing pipe that transforms bytes into NATS frames.
    *
    * @tparam F
    *   The effect type
    * @return
    *   A Pipe that parses bytes into NatsFrame values
    */
  def parseStream[F[_]: Async]: Pipe[F, Byte, NatsFrame] =
    parseStream(ParserConfig.default)

  /** Create a parsing pipe with custom configuration.
    *
    * @param config
    *   Parser configuration
    * @tparam F
    *   The effect type
    * @return
    *   A Pipe that parses bytes into NatsFrame values
    */
  def parseStream[F[_]: Async](config: ParserConfig): Pipe[F, Byte, NatsFrame] =
    input =>
      Stream.suspend {
        // Fresh carry per materialization, born inside the stream and owned by
        // the single fiber that pulls it.
        val state = new ParserState(InitialCapacity)
        go(config, state, input).stream
      }

  /** Drive parsing: emit as many complete frames as the current carry allows,
    * pulling more input whenever a frame can't yet be completed.
    */
  private def go[F[_]: Async](
      config: ParserConfig,
      st: ParserState,
      input: Stream[F, Byte]
  ): Pull[F, NatsFrame, Unit] =
    parseOne(config, st) match
      case Step.Emit(frame) =>
        Pull.output1(frame) >> go(config, st, input)
      case Step.NeedMoreControl =>
        needMore(config, st, input, -1)
      case Step.NeedMorePayload(length) =>
        needMore(config, st, input, length)
      case Step.FailSoft(message) =>
        emitParseError(config, message)
      case Step.FailHard(message) =>
        Pull.raiseError(fs2.nats.errors.NatsError.ProtocolParseError(message))

  /** Pull one more upstream chunk into the carry and resume, or handle EOF.
    *
    * @param payloadLen
    *   `>= 0` if we were waiting on a payload of that length (so EOF is a
    *   payload-truncation error), `-1` if we were waiting on a control line.
    */
  private def needMore[F[_]: Async](
      config: ParserConfig,
      st: ParserState,
      input: Stream[F, Byte],
      payloadLen: Int
  ): Pull[F, NatsFrame, Unit] =
    input.pull.uncons.flatMap {
      case Some((chunk, rest)) =>
        val live = st.end - st.start
        if live.toLong + chunk.size.toLong > MaxArray.toLong then
          Pull.raiseError(
            fs2.nats.errors.NatsError.ProtocolParseError(
              s"Frame exceeds maximum buffer size of $MaxArray bytes"
            )
          )
        else
          appendChunk(st, chunk)
          go(config, st, rest)

      case None =>
        if payloadLen >= 0 then
          Pull.raiseError(
            fs2.nats.errors.NatsError.ProtocolParseError(
              s"Unexpected end of stream while reading payload (expected $payloadLen bytes)"
            )
          )
        else if st.end == st.start then Pull.done
        else if config.strictMode then
          Pull.raiseError(
            fs2.nats.errors.NatsError.ProtocolParseError(
              "Incomplete control line at end of stream"
            )
          )
        else
          Pull.output1(
            NatsFrame.ParseErrorFrame(
              "Incomplete control line at end of stream"
            )
          ) >> Pull.done
    }

  /** Append an upstream chunk to the carry with a single `arraycopy`, growing
    * (doubling) or compacting as needed. Caller guarantees `live + chunk.size`
    * fits within `MaxArray`.
    */
  private def appendChunk(st: ParserState, chunk: Chunk[Byte]): Unit =
    val n = chunk.size
    if n > 0 then
      ensureCapacity(st, n)
      chunk.copyToArray(st.carry, st.end)
      st.end += n

  /** Guarantee room for `extra` more bytes at `end`. Compacts (slides the live
    * window to index 0) before growing, so the carry stays bounded by the
    * largest single in-flight frame rather than the cumulative stream size.
    */
  private def ensureCapacity(st: ParserState, extra: Int): Unit =
    val live = st.end - st.start
    val required = live + extra
    if required <= st.carry.length then
      if st.end + extra > st.carry.length then compact(st)
    else
      var newCap = st.carry.length
      while newCap < required do newCap = growStep(newCap)
      val next = new Array[Byte](newCap)
      System.arraycopy(st.carry, st.start, next, 0, live)
      st.carry = next
      st.start = 0
      st.end = live

  private def compact(st: ParserState): Unit =
    if st.start > 0 then
      val live = st.end - st.start
      System.arraycopy(st.carry, st.start, st.carry, 0, live)
      st.start = 0
      st.end = live

  private def growStep(cur: Int): Int =
    if cur >= MaxArray / 2 then MaxArray else cur * 2

  /** Attempt to parse exactly one frame from `[start, end)`. Advances
    * `st.start` past whatever it consumes (including skipped blank lines).
    */
  private def parseOne(config: ParserConfig, st: ParserState): Step =
    var result: Step = null
    while result == null do
      val start = st.start
      val crIdx = findCrlf(st.carry, start, st.end)
      if crIdx < 0 then
        if (st.end - start) > config.maxControlLineLength then
          result = Step.FailSoft(
            s"Control line exceeds maximum length of ${config.maxControlLineLength}"
          )
        else result = Step.NeedMoreControl
      else
        val controlLen = crIdx - start
        if controlLen > config.maxControlLineLength then
          result = Step.FailSoft(
            s"Control line exceeds maximum length of ${config.maxControlLineLength}"
          )
        else
          val tokens = tokenize(st.carry, start, crIdx)
          tokens.headOption.map(_.toUpperCase) match
            case None =>
              // Blank control line: skip past it and keep scanning.
              st.start = crIdx + 2

            case Some("INFO") =>
              val jsonStr = decodeLine(st.carry, start, crIdx).drop(5).trim
              Try(readFromString[Info](jsonStr)) match
                case Success(info) =>
                  st.start = crIdx + 2
                  result = Step.Emit(NatsFrame.InfoFrame(info))
                case Failure(err) =>
                  result = Step.FailSoft(
                    s"Failed to parse INFO JSON: ${err.getMessage}"
                  )

            case Some("MSG") =>
              result = handleMsg(config, st, tokens, crIdx)

            case Some("HMSG") =>
              result = handleHMsg(config, st, tokens, crIdx)

            case Some("PING") =>
              st.start = crIdx + 2
              result = Step.Emit(NatsFrame.PingFrame)

            case Some("PONG") =>
              st.start = crIdx + 2
              result = Step.Emit(NatsFrame.PongFrame)

            case Some("+OK") =>
              st.start = crIdx + 2
              result = Step.Emit(NatsFrame.OkFrame)

            case Some("-ERR") =>
              val msg = decodeLine(st.carry, start, crIdx)
                .drop(4)
                .trim
                .stripPrefix("'")
                .stripSuffix("'")
              st.start = crIdx + 2
              result = Step.Emit(NatsFrame.ErrFrame(msg))

            case Some(cmd) =>
              result = Step.FailSoft(s"Unrecognized command: $cmd")
    result

  private def handleMsg(
      config: ParserConfig,
      st: ParserState,
      tokens: Vector[String],
      crIdx: Int
  ): Step =
    tokens match
      case Vector("MSG", subject, sidStr, replyTo, lengthStr) =>
        (parseLong(sidStr), parseInt(lengthStr)) match
          case (Some(sid), Some(length)) =>
            validatePayloadLength(config, length.toLong) match
              case Some(errMsg) => Step.FailSoft(errMsg)
              case None         =>
                readPayload(
                  config,
                  st,
                  crIdx,
                  length,
                  (pStart, pEnd) =>
                    Step.Emit(
                      NatsFrame.MsgFrame(
                        subject,
                        sid,
                        Some(replyTo),
                        Chunk.array(
                          java.util.Arrays.copyOfRange(st.carry, pStart, pEnd)
                        )
                      )
                    )
                )
          case _ =>
            Step.FailSoft(s"Invalid MSG frame: ${tokens.mkString(" ")}")

      case Vector("MSG", subject, sidStr, lengthStr) =>
        (parseLong(sidStr), parseInt(lengthStr)) match
          case (Some(sid), Some(length)) =>
            validatePayloadLength(config, length.toLong) match
              case Some(errMsg) => Step.FailSoft(errMsg)
              case None         =>
                readPayload(
                  config,
                  st,
                  crIdx,
                  length,
                  (pStart, pEnd) =>
                    Step.Emit(
                      NatsFrame.MsgFrame(
                        subject,
                        sid,
                        None,
                        Chunk.array(
                          java.util.Arrays.copyOfRange(st.carry, pStart, pEnd)
                        )
                      )
                    )
                )
          case _ =>
            Step.FailSoft(s"Invalid MSG frame: ${tokens.mkString(" ")}")

      case _ =>
        Step.FailSoft(s"Invalid MSG frame format: ${tokens.mkString(" ")}")

  private def handleHMsg(
      config: ParserConfig,
      st: ParserState,
      tokens: Vector[String],
      crIdx: Int
  ): Step =
    tokens match
      case Vector(
            "HMSG",
            subject,
            sidStr,
            replyTo,
            headerLenStr,
            totalLenStr
          ) =>
        (parseLong(sidStr), parseInt(headerLenStr), parseInt(totalLenStr)) match
          case (Some(sid), Some(headerLen), Some(totalLen)) =>
            validatePayloadLength(config, totalLen.toLong) match
              case Some(errMsg) => Step.FailSoft(errMsg)
              case None         =>
                readPayload(
                  config,
                  st,
                  crIdx,
                  totalLen,
                  (pStart, _) =>
                    buildHMsg(
                      st,
                      subject,
                      sid,
                      Some(replyTo),
                      headerLen,
                      totalLen,
                      pStart
                    )
                )
          case _ =>
            Step.FailSoft(s"Invalid HMSG frame: ${tokens.mkString(" ")}")

      case Vector("HMSG", subject, sidStr, headerLenStr, totalLenStr) =>
        (parseLong(sidStr), parseInt(headerLenStr), parseInt(totalLenStr)) match
          case (Some(sid), Some(headerLen), Some(totalLen)) =>
            validatePayloadLength(config, totalLen.toLong) match
              case Some(errMsg) => Step.FailSoft(errMsg)
              case None         =>
                readPayload(
                  config,
                  st,
                  crIdx,
                  totalLen,
                  (pStart, _) =>
                    buildHMsg(
                      st,
                      subject,
                      sid,
                      None,
                      headerLen,
                      totalLen,
                      pStart
                    )
                )
          case _ =>
            Step.FailSoft(s"Invalid HMSG frame: ${tokens.mkString(" ")}")

      case _ =>
        Step.FailSoft(s"Invalid HMSG frame format: ${tokens.mkString(" ")}")

  /** Build an HMSG frame by splitting the already-buffered payload region into
    * its header and body parts (each copied into a fresh immutable Chunk). The
    * header/body split clamps like `Chunk.take`/`Chunk.drop` so odd
    * `headerLen`/`totalLen` combinations behave exactly as before.
    */
  private def buildHMsg(
      st: ParserState,
      subject: String,
      sid: Long,
      replyTo: Option[String],
      headerLen: Int,
      totalLen: Int,
      pStart: Int
  ): Step =
    val h = math.max(0, math.min(headerLen, totalLen))
    val headerChunk =
      Chunk.array(java.util.Arrays.copyOfRange(st.carry, pStart, pStart + h))
    val payloadChunk =
      Chunk.array(
        java.util.Arrays.copyOfRange(st.carry, pStart + h, pStart + totalLen)
      )
    Headers.parseWithStatus(headerChunk) match
      case Right((statusCode, statusDescription, headers)) =>
        Step.Emit(
          NatsFrame.HMsgFrame(
            subject,
            sid,
            replyTo,
            headers,
            statusCode,
            statusDescription,
            payloadChunk
          )
        )
      case Left(err) =>
        Step.FailSoft(s"Failed to parse HMSG headers: $err")

  /** Check whether a `length`-byte payload (plus its trailing CRLF) is fully
    * buffered after the control-line CRLF at `crIdx`. If so, verify the CRLF,
    * advance `st.start` past the payload, and delegate frame construction to
    * `build(payloadStart, payloadEnd)`. Otherwise signal `NeedMorePayload`.
    */
  private def readPayload(
      config: ParserConfig,
      st: ParserState,
      crIdx: Int,
      length: Int,
      build: (Int, Int) => Step
  ): Step =
    val payloadStart = crIdx + 2
    val needed = length.toLong + 2L
    if (st.end - payloadStart).toLong >= needed then
      val payloadEnd = payloadStart + length
      val crlfA = st.carry(payloadEnd)
      val crlfB = st.carry(payloadEnd + 1)
      if crlfA == CR && crlfB == LF then
        st.start = payloadEnd + 2
        build(payloadStart, payloadEnd)
      else if config.strictCRLF then
        Step.FailHard(
          s"Missing CRLF after payload (got 0x${String.format("%02x", crlfA)} 0x${String.format("%02x", crlfB)})"
        )
      else
        st.start = payloadEnd + 2
        build(payloadStart, payloadEnd)
    else Step.NeedMorePayload(length)

  /** Validate a declared payload length against config limits. Returns the
    * error message to fail with, or `None` if the length is acceptable.
    */
  private def validatePayloadLength(
      config: ParserConfig,
      length: Long
  ): Option[String] =
    config.maxPayloadLimit match
      case Some(max) if length > max =>
        Some(s"Payload size $length exceeds maximum of $max")
      case _ if length < 0 =>
        Some(s"Invalid negative payload length: $length")
      case _ =>
        None

  private def emitParseError[F[_]: Async](
      config: ParserConfig,
      message: String
  ): Pull[F, NatsFrame, Unit] =
    if config.strictMode then
      Pull.raiseError(fs2.nats.errors.NatsError.ProtocolParseError(message))
    else Pull.output1(NatsFrame.ParseErrorFrame(message)) >> Pull.done

  /** Scan `[start, end)` of `carry` for the first CRLF, returning the absolute
    * index of the CR, or `-1` if none is present in the window.
    */
  private def findCrlf(carry: Array[Byte], start: Int, end: Int): Int =
    var i = start
    val limit = end - 1
    while i < limit do
      if carry(i) == CR && carry(i + 1) == LF then return i
      i += 1
    -1

  private def isSpace(b: Byte): Boolean =
    b == ' '.toByte || b == '\t'.toByte

  /** Decode `[start, end)` of `carry` as a UTF-8 String. Used only for the
    * infrequent INFO and -ERR frames.
    */
  private def decodeLine(carry: Array[Byte], start: Int, end: Int): String =
    new String(carry, start, end - start, StandardCharsets.UTF_8)

  /** Split `[start, end)` of `carry` into whitespace-delimited tokens at the
    * byte level, avoiding a whole-line String allocation on the MSG/HMSG hot
    * path.
    */
  private def tokenize(
      carry: Array[Byte],
      start: Int,
      end: Int
  ): Vector[String] =
    val builder = Vector.newBuilder[String]
    var i = start
    while i < end do
      while i < end && isSpace(carry(i)) do i += 1
      if i < end then
        val tokenStart = i
        while i < end && !isSpace(carry(i)) do i += 1
        builder += new String(
          carry,
          tokenStart,
          i - tokenStart,
          StandardCharsets.UTF_8
        )
    builder.result()

  private def parseInt(s: String): Option[Int] =
    try Some(s.toInt)
    catch case _: NumberFormatException => None

  private def parseLong(s: String): Option[Long] =
    try Some(s.toLong)
    catch case _: NumberFormatException => None
