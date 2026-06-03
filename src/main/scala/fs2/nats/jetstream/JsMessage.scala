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
  * Two on-wire formats are normalized to shared indices: V1 (9 tokens, no
  * domain/account) has two empty tokens inserted at positions 2-3; V2 (>=11
  * tokens; the server sends 12) already carries domain + account + a trailing
  * random token. A token count of 10 or fewer than 9 is malformed.
  */
private[jetstream] object JsAckParser:

  def parse(reply: String): Either[String, JsMetadata] =
    val raw = reply.split("\\.", -1)
    if raw.length < 2 || raw(0) != "$JS" || raw(1) != "ACK" then
      Left(s"Not a JetStream ACK subject: '$reply'")
    else
      val tokens =
        if raw.length == 9 then Some(raw.patch(2, Array("", ""), 0))
        else if raw.length >= 11 then Some(raw)
        else None
      tokens match
        case None =>
          Left(
            s"Malformed JetStream ACK subject ('$reply'): ${raw.length} tokens"
          )
        case Some(t) =>
          for
            numDelivered <- parseLong(t(6), "num_delivered", reply)
            streamSeq <- parseLong(t(7), "stream_seq", reply)
            consumerSeq <- parseLong(t(8), "consumer_seq", reply)
            tsNanos <- parseLong(t(9), "timestamp", reply)
            numPending <- parseLong(t(10), "num_pending", reply)
          yield JsMetadata(
            stream = t(4),
            consumer = t(5),
            streamSeq = streamSeq,
            consumerSeq = consumerSeq,
            numDelivered = numDelivered,
            numPending = numPending,
            timestamp = Instant.ofEpochSecond(0L, tsNanos),
            domain = Option(t(2)).filter(d => d.nonEmpty && d != "_")
          )

  private def parseLong(
      s: String,
      field: String,
      reply: String
  ): Either[String, Long] =
    s.toLongOption.toRight(s"Invalid $field in ACK subject '$reply': '$s'")
