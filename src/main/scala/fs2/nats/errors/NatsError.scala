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

package fs2.nats.errors

/** Sealed ADT representing all error types that can occur in the NATS client.
  * These errors are surfaced through the client API and events stream.
  */
sealed abstract class NatsError(
    message: String,
    cause: Option[Throwable] = None
) extends Exception(message, cause.orNull)

object NatsError:

  /** Payload exceeds the server's max_payload limit. Check Info.maxPayload and
    * ensure messages are within bounds.
    *
    * @param size
    *   The attempted payload size in bytes
    * @param maxPayload
    *   The server's maximum allowed payload size
    */
  final case class PayloadTooLarge(size: Long, maxPayload: Long)
      extends NatsError(
        s"Payload size $size bytes exceeds server max_payload of $maxPayload bytes"
      )

  /** Error parsing NATS protocol data from the server. May indicate protocol
    * version mismatch or corrupted data.
    *
    * @param description
    *   Description of what failed to parse
    * @param cause
    *   Optional underlying parsing exception
    */
  final case class ProtocolParseError(
      description: String,
      cause: Option[Throwable] = None
  ) extends NatsError(s"Protocol parse error: $description", cause)

  /** Authentication or authorization failed. Check credentials configuration.
    *
    * @param serverMessage
    *   The error message from the server
    */
  final case class AuthorizationError(serverMessage: String)
      extends NatsError(s"Authorization failed: $serverMessage")

  /** Failed to establish or maintain connection to NATS server. May occur on
    * initial connect or during reconnection attempts.
    *
    * @param description
    *   Description of the connection failure
    * @param cause
    *   Optional underlying network exception
    */
  final case class ConnectionFailed(
      description: String,
      cause: Option[Throwable] = None
  ) extends NatsError(s"Connection failed: $description", cause)

  /** Subscription queue is full and slow consumer policy triggered. Either
    * increase queue capacity or improve consumer throughput.
    *
    * @param sid
    *   The subscription ID experiencing slow consumption
    * @param subject
    *   The subject of the subscription
    * @param queueSize
    *   The current queue size
    */
  final case class SlowConsumer(sid: Long, subject: String, queueSize: Int)
      extends NatsError(
        s"Slow consumer on subscription $sid ($subject): queue full at $queueSize messages"
      )

  /** Operation timeout expired.
    *
    * @param operation
    *   Description of the operation that timed out
    * @param timeoutMs
    *   The timeout duration in milliseconds
    */
  final case class Timeout(operation: String, timeoutMs: Long)
      extends NatsError(s"Timeout after ${timeoutMs}ms: $operation")

  /** Client has been closed and operation cannot proceed.
    */
  case object ClientClosed extends NatsError("Client has been closed")

  /** TLS handshake or configuration error.
    *
    * @param description
    *   Description of the TLS error
    * @param cause
    *   Optional underlying TLS exception
    */
  final case class TlsError(
      description: String,
      cause: Option[Throwable] = None
  ) extends NatsError(s"TLS error: $description", cause)

  /** Invalid subject format. Subjects cannot contain spaces or certain special
    * characters.
    *
    * @param subject
    *   The invalid subject string
    * @param reason
    *   Why the subject is invalid
    */
  final case class InvalidSubject(subject: String, reason: String)
      extends NatsError(s"Invalid subject '$subject': $reason")

  /** Server protocol error received. The server sent an -ERR response.
    *
    * @param serverMessage
    *   The error message from the server
    */
  final case class ServerError(serverMessage: String)
      extends NatsError(s"Server error: $serverMessage")

  /** Maximum reconnection attempts exhausted.
    *
    * @param attempts
    *   The number of attempts made
    * @param lastError
    *   The last error encountered
    */
  final case class MaxReconnectsExceeded(
      attempts: Int,
      lastError: Option[Throwable]
  ) extends NatsError(
        s"Maximum reconnection attempts ($attempts) exceeded",
        lastError
      )
