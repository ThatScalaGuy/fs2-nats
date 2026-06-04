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

/** Acknowledgement returned by the server for a successful JetStream publish.
  *
  * @param stream
  *   The stream that stored the message
  * @param seq
  *   The assigned stream sequence number
  * @param duplicate
  *   True when the publish was de-duplicated (still a success, not an error)
  * @param domain
  *   The JetStream domain, if any
  */
final case class PubAck(
    stream: String,
    seq: Long,
    duplicate: Boolean = false,
    domain: Option[String] = None
)

object PubAck:
  given JsonValueCodec[PubAck] = JsonCodecMaker.make(JsWire.snake)
