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

package fs2.nats.micro

import java.time.Instant
import scala.concurrent.duration.FiniteDuration

/** Static description of a running service instance (ADR-32 `INFO`). */
final class ServiceInfo private[micro] (
    val name: String,
    val id: String,
    val version: String,
    val description: Option[String],
    val metadata: Map[String, String],
    val endpoints: List[EndpointInfo]
)

/** Static description of one endpoint. `subject` is the wildcard form. */
final class EndpointInfo private[micro] (
    val name: String,
    val subject: String,
    val queueGroup: String,
    val metadata: Map[String, String]
)

/** Runtime statistics of a service instance (ADR-32 `STATS`). */
final class ServiceStats private[micro] (
    val name: String,
    val id: String,
    val version: String,
    val started: Instant,
    val endpoints: List[EndpointStats]
)

/** Runtime statistics of one endpoint. */
final class EndpointStats private[micro] (
    val name: String,
    val subject: String,
    val queueGroup: String,
    val numRequests: Long,
    val numErrors: Long,
    val lastError: Option[String],
    val processingTime: FiniteDuration,
    val averageProcessingTime: FiniteDuration
)
