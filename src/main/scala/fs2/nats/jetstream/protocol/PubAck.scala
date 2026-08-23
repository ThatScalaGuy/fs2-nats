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

/** Decode-only mirror of [[PubAck]] that also carries the `error` envelope the
  * server sends inline on a failed publish, so the publish path parses the
  * reply once instead of probing for the envelope and then re-reading it. Every
  * field is defaulted because an error reply carries none of them; `stream` and
  * `seq` use sentinels so a truncated success reply is still rejected the way
  * [[PubAck]]'s required fields reject it today.
  */
private[jetstream] final case class PubAckResponse(
    stream: String = null,
    seq: Long = -1L,
    duplicate: Boolean = false,
    domain: Option[String] = None,
    error: Option[ApiError] = None
)

private[jetstream] object PubAckResponse:
  given JsonValueCodec[PubAckResponse] = JsonCodecMaker.make(JsWire.snake)
