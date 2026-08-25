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

/** Configuration for a [[NatsService]] instance.
  *
  * @param name
  *   Service name, `[A-Za-z0-9-_]+`
  * @param version
  *   SemVer version string
  */
final class ServiceConfig private (
    val name: String,
    val version: String,
    val description: Option[String],
    val metadata: Map[String, String],
    val queueGroup: String,
    val maxConcurrent: Int
):

  private def copy(
      description: Option[String] = description,
      metadata: Map[String, String] = metadata,
      queueGroup: String = queueGroup,
      maxConcurrent: Int = maxConcurrent
  ): ServiceConfig =
    new ServiceConfig(
      name,
      version,
      description,
      metadata,
      queueGroup,
      maxConcurrent
    )

  def withDescription(d: String): ServiceConfig = copy(description = Some(d))
  def withMetadata(m: Map[String, String]): ServiceConfig = copy(metadata = m)
  def withQueueGroup(q: String): ServiceConfig = copy(queueGroup = q)
  def withMaxConcurrent(n: Int): ServiceConfig = copy(maxConcurrent = n)

object ServiceConfig:

  /** Default queue group `"q"` (ADR-32), max 64 concurrent handlers. */
  def apply(name: String, version: String): ServiceConfig =
    new ServiceConfig(name, version, None, Map.empty, "q", 64)
