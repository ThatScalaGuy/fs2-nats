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

package fs2.nats.micro.protocol

import com.github.plokhotnyuk.jsoniter_scala.core.*
import com.github.plokhotnyuk.jsoniter_scala.macros.*

import java.time.Instant

/** Shared jsoniter wire-encoding config for the ADR-32 discovery JSON:
  * snake_case field names, absent-if-empty collections/options (jsoniter's
  * defaults), durations pre-converted to int64 nanoseconds, `started` via the
  * built-in ISO-8601 `Instant` codec.
  */
private[micro] object MicroWire:
  // transientDefault must be off: the `type` tag always equals its declared
  // default and would otherwise be dropped from the wire (ADR-32 tooling keys
  // on it). transientEmpty/transientNone still omit empty maps/lists/None.
  inline def snake: CodecMakerConfig =
    CodecMakerConfig
      .withFieldNameMapper(JsonCodecMaker.enforce_snake_case)
      .withTransientDefault(false)

/** ADR-32 error header names. */
private[micro] object MicroHeaders:
  val ErrorCode = "Nats-Service-Error-Code"
  val Error = "Nats-Service-Error"

/** `$SRV.PING` response, type `io.nats.micro.v1.ping_response`. */
private[micro] final case class PingResponse(
    `type`: String = PingResponse.Tag,
    name: String,
    id: String,
    version: String,
    metadata: Map[String, String] = Map.empty
)

private[micro] object PingResponse:
  val Tag = "io.nats.micro.v1.ping_response"
  given codec: JsonValueCodec[PingResponse] =
    JsonCodecMaker.make(MicroWire.snake)

private[micro] final case class WireEndpointInfo(
    name: String,
    subject: String,
    queueGroup: String,
    metadata: Map[String, String] = Map.empty
)

/** `$SRV.INFO` response, type `io.nats.micro.v1.info_response`. */
private[micro] final case class InfoResponse(
    `type`: String = InfoResponse.Tag,
    name: String,
    id: String,
    version: String,
    metadata: Map[String, String] = Map.empty,
    description: Option[String] = None,
    endpoints: List[WireEndpointInfo] = Nil
)

private[micro] object InfoResponse:
  val Tag = "io.nats.micro.v1.info_response"
  given codec: JsonValueCodec[InfoResponse] =
    JsonCodecMaker.make(MicroWire.snake)

private[micro] final case class WireEndpointStats(
    name: String,
    subject: String,
    queueGroup: String,
    numRequests: Long,
    numErrors: Long,
    lastError: Option[String] = None,
    processingTime: Long,
    averageProcessingTime: Long
)

/** `$SRV.STATS` response, type `io.nats.micro.v1.stats_response`. */
private[micro] final case class StatsResponse(
    `type`: String = StatsResponse.Tag,
    name: String,
    id: String,
    version: String,
    metadata: Map[String, String] = Map.empty,
    started: Instant,
    endpoints: List[WireEndpointStats] = Nil
)

private[micro] object StatsResponse:
  val Tag = "io.nats.micro.v1.stats_response"
  given codec: JsonValueCodec[StatsResponse] =
    JsonCodecMaker.make(MicroWire.snake)
