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

package fs2.nats.jetstream.protocol

import io.circe.{Encoder, Json}

import scala.concurrent.duration.FiniteDuration

/** A pull `CONSUMER.MSG.NEXT` request body. */
final case class PullRequest(
    batch: Int,
    expires: FiniteDuration,
    maxBytes: Option[Int] = None,
    noWait: Boolean = false,
    idleHeartbeat: Option[FiniteDuration] = None
)

object PullRequest:
  given Encoder[PullRequest] = Encoder.instance { r =>
    JsWire.obj(
      Some("batch" -> Json.fromInt(r.batch)),
      Some("expires" -> JsWire.durationToNanos(r.expires)),
      r.maxBytes.map(b => "max_bytes" -> Json.fromInt(b)),
      Option.when(r.noWait)("no_wait" -> Json.fromBoolean(true)),
      r.idleHeartbeat.map(d => "idle_heartbeat" -> JsWire.durationToNanos(d))
    )
  }
