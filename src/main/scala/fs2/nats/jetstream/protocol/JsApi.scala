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

/** The error envelope present on a failed JetStream API response:
  * `{"type":…,"error":{"code":<http>,"err_code":<uint16>,"description":…}}`.
  *
  * @param code
  *   HTTP-like status code
  * @param errCode
  *   JetStream-specific numeric error code
  * @param description
  *   Human-readable error description
  */
final case class ApiError(code: Int, errCode: Int = 0, description: String = "")

object ApiError:
  given JsonValueCodec[ApiError] = JsonCodecMaker.make(JsWire.snake)

/** A minimal `{"success": <bool>}` response used by delete/purge-style API
  * calls that carry no other payload.
  */
final case class SuccessResponse(success: Boolean = false)

object SuccessResponse:
  given JsonValueCodec[SuccessResponse] = JsonCodecMaker.make(JsWire.snake)

/** Reads just the `error` envelope from any API response, ignoring the success
  * payload (jsoniter skips unexpected fields by default). The first of the
  * two-pass decode used by [[fs2.nats.jetstream.JetStream]].
  */
private[jetstream] final case class ApiErrorEnvelope(
    error: Option[ApiError] = None
)
private[jetstream] object ApiErrorEnvelope:
  given JsonValueCodec[ApiErrorEnvelope] = JsonCodecMaker.make(JsWire.snake)

/** Wrapper for `STREAM.MSG.GET` responses, whose payload lives under `message`.
  */
private[jetstream] final case class GetMsgResponse(message: StoredMessage)
private[jetstream] object GetMsgResponse:
  given JsonValueCodec[GetMsgResponse] = JsonCodecMaker.make(JsWire.snake)
