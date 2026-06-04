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

import com.github.plokhotnyuk.jsoniter_scala.core.*
import com.github.plokhotnyuk.jsoniter_scala.macros.*
import fs2.nats.jetstream.protocol.JsWire.given

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
  given JsonValueCodec[PullRequest] = JsonCodecMaker.make(JsWire.snake)
