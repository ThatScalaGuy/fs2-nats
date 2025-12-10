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

import fs2.Chunk

/** Represents a complete parsed frame from the NATS protocol stream. Each
  * variant corresponds to a specific NATS protocol message.
  *
  * The parser emits these frames which are then processed by the connection
  * manager.
  */
sealed trait NatsFrame

object NatsFrame:

  /** INFO frame received from the server. Sent upon initial connection and
    * asynchronously when cluster topology changes.
    *
    * @param info
    *   The parsed server INFO message
    */
  final case class InfoFrame(info: Info) extends NatsFrame

  /** MSG frame - message delivery without headers. Received when a subscribed
    * subject receives a message.
    *
    * @param subject
    *   The subject the message was published to
    * @param sid
    *   The subscription ID that matched
    * @param replyTo
    *   Optional reply-to subject for request/reply pattern
    * @param payload
    *   The message payload bytes
    */
  final case class MsgFrame(
      subject: String,
      sid: Long,
      replyTo: Option[String],
      payload: Chunk[Byte]
  ) extends NatsFrame

  /** HMSG frame - message delivery with headers (NATS 2.2+). Received when a
    * subscribed subject receives a message with headers.
    *
    * @param subject
    *   The subject the message was published to
    * @param sid
    *   The subscription ID that matched
    * @param replyTo
    *   Optional reply-to subject for request/reply pattern
    * @param headers
    *   The parsed message headers
    * @param statusCode
    *   Optional status code from header version line (e.g., 503 for no
    *   responders)
    * @param payload
    *   The message payload bytes
    */
  final case class HMsgFrame(
      subject: String,
      sid: Long,
      replyTo: Option[String],
      headers: Headers,
      statusCode: Option[Int],
      payload: Chunk[Byte]
  ) extends NatsFrame

  /** PING frame from server. Client must respond with PONG to keep connection
    * alive.
    */
  case object PingFrame extends NatsFrame

  /** PONG frame from server. Response to client PING, confirms connection is
    * alive.
    */
  case object PongFrame extends NatsFrame

  /** +OK frame from server. Only sent when verbose mode is enabled in CONNECT.
    */
  case object OkFrame extends NatsFrame

  /** -ERR frame from server. Indicates a protocol error. Connection may be
    * terminated by server.
    *
    * @param message
    *   The error message from the server
    */
  final case class ErrFrame(message: String) extends NatsFrame

  /** Parse error frame - emitted when parser encounters invalid protocol data.
    * This is an internal frame type not from the NATS protocol itself.
    *
    * @param message
    *   Description of the parse error
    * @param cause
    *   Optional underlying exception
    */
  final case class ParseErrorFrame(
      message: String,
      cause: Option[Throwable] = None
  ) extends NatsFrame
