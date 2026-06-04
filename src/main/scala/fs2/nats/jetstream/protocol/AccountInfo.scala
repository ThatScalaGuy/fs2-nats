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

/** Per-account JetStream resource limits. `-1` means unlimited. */
final case class AccountLimits(
    maxMemory: Long = 0L,
    maxStorage: Long = 0L,
    maxStreams: Int = -1,
    maxConsumers: Int = -1
)
object AccountLimits:
  given JsonValueCodec[AccountLimits] = JsonCodecMaker.make(JsWire.snake)

/** Account-level JetStream usage and limits (`$JS.API.INFO`). */
final case class AccountInfo(
    memory: Long = 0L,
    storage: Long = 0L,
    streams: Int = 0,
    consumers: Int = 0,
    domain: Option[String] = None,
    limits: AccountLimits = AccountLimits()
)
object AccountInfo:
  given JsonValueCodec[AccountInfo] = JsonCodecMaker.make(JsWire.snake)
