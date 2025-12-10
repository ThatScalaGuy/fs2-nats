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
  */
final case class TransportConfig(
    writeQueueCapacity: Int = 8192,
    connectTimeout: FiniteDuration = 10.seconds,
    writeTimeout: FiniteDuration = 30.seconds
)

object TransportConfig:
  val default: TransportConfig = TransportConfig()

  val highThroughput: TransportConfig = TransportConfig(
    writeQueueCapacity = 65536
  )

  val lowLatency: TransportConfig = TransportConfig(
    writeQueueCapacity = 1024
  )
