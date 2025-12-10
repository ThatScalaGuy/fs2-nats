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

package fs2.nats.client

import fs2.nats.protocol.Info

/** Events emitted by the NATS client for observability. These events are
  * available through NatsClient.events stream.
  */
sealed trait ClientEvent

object ClientEvent:

  /** Successfully connected to the NATS server.
    *
    * @param serverInfo
    *   The server INFO received upon connection
    */
  final case class Connected(serverInfo: Info) extends ClientEvent

  /** Disconnected from the NATS server.
    *
    * @param reason
    *   Description of why the disconnection occurred
    * @param willReconnect
    *   Whether the client will attempt to reconnect
    */
  final case class Disconnected(
      reason: String,
      willReconnect: Boolean
  ) extends ClientEvent

  /** Successfully reconnected to the NATS server after a disconnection.
    *
    * @param serverInfo
    *   The server INFO from the new connection
    * @param attempt
    *   The reconnection attempt number that succeeded
    */
  final case class Reconnected(
      serverInfo: Info,
      attempt: Int
  ) extends ClientEvent

  /** Attempting to reconnect to the NATS server.
    *
    * @param attempt
    *   The current attempt number
    * @param delayMs
    *   The delay in milliseconds before this attempt
    */
  final case class Reconnecting(
      attempt: Int,
      delayMs: Long
  ) extends ClientEvent

  /** A protocol error occurred.
    *
    * @param message
    *   The error message
    * @param fatal
    *   Whether this error is fatal and will cause disconnection
    */
  final case class ProtocolError(
      message: String,
      fatal: Boolean
  ) extends ClientEvent

  /** A subscription is consuming messages slower than they arrive.
    *
    * @param sid
    *   The subscription ID
    * @param subject
    *   The subscription subject
    * @param dropped
    *   The number of messages dropped (if applicable)
    */
  final case class SlowConsumer(
      sid: Long,
      subject: String,
      dropped: Int
  ) extends ClientEvent

  /** Server entered Lame Duck Mode and will shut down soon. Client should
    * prepare for disconnection.
    */
  case object LameDuckMode extends ClientEvent

  /** Server INFO was updated (e.g., cluster topology change).
    *
    * @param serverInfo
    *   The updated server INFO
    */
  final case class ServerInfoUpdated(serverInfo: Info) extends ClientEvent

  /** Maximum reconnection attempts have been exhausted.
    *
    * @param attempts
    *   The total number of attempts made
    * @param lastError
    *   The last error message encountered
    */
  final case class MaxReconnectsExceeded(
      attempts: Int,
      lastError: String
  ) extends ClientEvent
