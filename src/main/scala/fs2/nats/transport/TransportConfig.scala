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

package fs2.nats.transport

import fs2.io.net.SocketOption

import scala.concurrent.duration.*

/** Configuration for the NATS transport layer.
  *
  * @param writeQueueCapacity
  *   Capacity of the outbound write queue (default 8192). Controls how many
  *   write operations can be buffered before backpressure is applied.
  * @param connectTimeout
  *   Timeout for initial connection (default 10 seconds)
  * @param writeTimeout
  *   Timeout for write operations (default 30 seconds). If a single write
  *   operation takes longer than this duration, the connection will be marked
  *   as failed and closed. This prevents indefinite hangs on stalled writes.
  * @param socketOptions
  *   Options applied to sockets this library dials, in list order (later
  *   entries win, so callers override a default by appending rather than by
  *   rebuilding the list). Defaults to
  *   [[TransportConfig.defaultSocketOptions]], which disables Nagle. Sockets
  *   handed in from outside - `NatsSocket.fromSocket` and
  *   `TlsTransport.fromTlsSocket` - are left exactly as the caller configured
  *   them.
  */
final case class TransportConfig(
    writeQueueCapacity: Int = 8192,
    connectTimeout: FiniteDuration = 10.seconds,
    writeTimeout: FiniteDuration = 30.seconds,
    socketOptions: List[SocketOption] = TransportConfig.defaultSocketOptions
)

object TransportConfig:

  /** Default socket options: `TCP_NODELAY` on.
    *
    * The writer fiber already drains the whole outbound queue into a single
    * `socket.write` per pass, so a second round of kernel-side coalescing buys
    * nothing and only adds up to a delayed-ACK interval (~40ms) of latency
    * whenever the previous segment is still unacknowledged - which is the
    * normal state for a stream of fire-and-forget publishes.
    *
    * Send/receive buffer sizes are deliberately left unset: an explicit
    * `SO_RCVBUF` turns off the kernel's receive-buffer autotuning and caps
    * throughput on high bandwidth-delay links. Callers who really want a fixed
    * size can append one.
    *
    * This is a single shared `val` rather than an inline default expression on
    * purpose: `SocketOption` instances do not implement structural equality, so
    * re-evaluating the list per construction would make two default-built
    * `TransportConfig`s compare unequal.
    */
  val defaultSocketOptions: List[SocketOption] = List(
    SocketOption.noDelay(true)
  )

  val default: TransportConfig = TransportConfig()

  val highThroughput: TransportConfig = TransportConfig(
    writeQueueCapacity = 65536
  )

  /** Small write queue, so each drain covers fewer messages and gets to the
    * wire sooner; combined with the default `TCP_NODELAY` this trades packet
    * rate for latency.
    */
  val lowLatency: TransportConfig = TransportConfig(
    writeQueueCapacity = 1024
  )
