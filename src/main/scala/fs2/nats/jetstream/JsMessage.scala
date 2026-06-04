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

package fs2.nats.jetstream

import fs2.Chunk
import fs2.nats.protocol.Headers

import java.nio.charset.StandardCharsets.UTF_8
import java.time.Instant
import scala.concurrent.duration.FiniteDuration

/** Parsed JetStream delivery metadata, derived from a message's `$JS.ACK` reply
  * subject (no extra round-trip).
  *
  * @param numDelivered
  *   delivery count (>=1; >1 means redelivery)
  */
final case class JsMetadata(
    stream: String,
    consumer: String,
    streamSeq: Long,
    consumerSeq: Long,
    numDelivered: Long,
    numPending: Long,
    timestamp: Instant,
    domain: Option[String]
):
  def isRedelivery: Boolean = numDelivered > 1

/** A message delivered by a JetStream consumer, with parsed metadata and full
  * ack semantics. The finalizing acks (`ack`/`ackSync`/`nak`/`nakWithDelay`/
  * `term`/`termWith`) take effect once; `inProgress` is repeatable.
  */
trait JsMessage[F[_]]:
  def subject: String
  def headers: Headers
  def payload: Chunk[Byte]
  def payloadAsString: String
  def metadata: JsMetadata

  /** The `$JS.ACK...` subject acks are sent to. */
  def ackReply: String

  /** Acknowledge (fire-and-forget `+ACK`). */
  def ack: F[Unit]

  /** Acknowledge and await server confirmation (double-ack). */
  def ackSync: F[Unit]

  /** Negatively acknowledge for immediate redelivery (`-NAK`). */
  def nak: F[Unit]

  /** Negatively acknowledge, requesting redelivery after `delay`. */
  def nakWithDelay(delay: FiniteDuration): F[Unit]

  /** Signal work-in-progress, resetting the ack-wait timer (`+WPI`). */
  def inProgress: F[Unit]

  /** Terminate delivery; do not redeliver (`+TERM`). */
  def term: F[Unit]

  /** Terminate delivery with a reason (`+TERM <reason>`). */
  def termWith(reason: String): F[Unit]

/** Raw ack payload bytes sent to the `$JS.ACK` reply subject. */
private[jetstream] object AckBytes:
  val Ack: Chunk[Byte] = ascii("+ACK")
  val Nak: Chunk[Byte] = ascii("-NAK")
  val InProgress: Chunk[Byte] = ascii("+WPI")
  val Term: Chunk[Byte] = ascii("+TERM")

  def nakWithDelay(delay: FiniteDuration): Chunk[Byte] =
    ascii(s"""-NAK {"delay": ${delay.toNanos}}""")

  def termWith(reason: String): Chunk[Byte] =
    ascii(s"+TERM $reason")

  private def ascii(s: String): Chunk[Byte] = Chunk.array(s.getBytes(UTF_8))

/** Parser for the `$JS.ACK` reply-subject metadata.
  *
  * Two on-wire formats share indices via an offset: V1 (9 tokens, no
  * domain/account) shifts the post-prefix fields left by two; V2 (>=11 tokens;
  * the server sends 12) carries domain + account + a trailing random token. A
  * token count of 10 or fewer than 9 is malformed.
  *
  * Hot path (runs per consumed message): a single delimiter scan records dot
  * positions, numeric fields are parsed directly from character ranges, and
  * only the stream/consumer/domain strings are materialized — avoiding the full
  * `split` (a substring per token) and the V1 normalization copy.
  */
private[nats] object JsAckParser:

  private val Prefix = "$JS.ACK."

  def parse(reply: String): Either[String, JsMetadata] =
    val len = reply.length
    if !reply.startsWith(Prefix) then
      Left(s"Not a JetStream ACK subject: '$reply'")
    else
      var nDots = 0
      var i = 0
      while i < len do
        if reply.charAt(i) == '.' then nDots += 1
        i += 1
      val tokenCount = nDots + 1
      if tokenCount != 9 && tokenCount < 11 then
        Left(s"Malformed JetStream ACK subject ('$reply'): $tokenCount tokens")
      else
        // V1 fields sit two indices earlier than V2 (no domain/account).
        val off = if tokenCount == 9 then -2 else 0
        val dots = new Array[Int](nDots)
        var k = 0
        var j = 0
        while j < len do
          if reply.charAt(j) == '.' then
            dots(k) = j
            k += 1
          j += 1

        def tStart(t: Int): Int = if t == 0 then 0 else dots(t - 1) + 1
        def tEnd(t: Int): Int = if t == nDots then len else dots(t)
        def num(field: String, t: Int): Either[String, Long] =
          parseULong(reply, tStart(t + off), tEnd(t + off))
            .toRight(s"Invalid $field in ACK subject '$reply'")

        for
          numDelivered <- num("num_delivered", 6)
          streamSeq <- num("stream_seq", 7)
          consumerSeq <- num("consumer_seq", 8)
          tsNanos <- num("timestamp", 9)
          numPending <- num("num_pending", 10)
        yield
          val domain =
            if off != 0 then None
            else
              val s = tStart(2)
              val e = tEnd(2)
              if e <= s || (e - s == 1 && reply.charAt(s) == '_') then None
              else Some(reply.substring(s, e))
          JsMetadata(
            stream = reply.substring(tStart(4 + off), tEnd(4 + off)),
            consumer = reply.substring(tStart(5 + off), tEnd(5 + off)),
            streamSeq = streamSeq,
            consumerSeq = consumerSeq,
            numDelivered = numDelivered,
            numPending = numPending,
            timestamp = Instant.ofEpochSecond(0L, tsNanos),
            domain = domain
          )

  /** Parse a non-negative decimal `Long` from `s[from until)` without
    * allocating an intermediate substring. Returns `None` on a non-digit or
    * empty range.
    */
  private def parseULong(s: String, from: Int, until: Int): Option[Long] =
    if from >= until then None
    else
      var v = 0L
      var i = from
      var ok = true
      while ok && i < until do
        val c = s.charAt(i)
        if c < '0' || c > '9' then ok = false
        else
          v = v * 10 + (c - '0').toLong
          i += 1
      if ok then Some(v) else None
