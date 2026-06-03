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

import io.circe.Decoder

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
final case class ApiError(code: Int, errCode: Int, description: String)

object ApiError:
  given Decoder[ApiError] = Decoder.instance { c =>
    for
      code <- c.downField("code").as[Int]
      errCode <- c.downField("err_code").as[Option[Int]].map(_.getOrElse(0))
      description <- c
        .downField("description")
        .as[Option[String]]
        .map(_.getOrElse(""))
    yield ApiError(code, errCode, description)
  }

/** A minimal `{"success": <bool>}` response used by delete/purge-style API
  * calls that carry no other payload.
  */
final case class SuccessResponse(success: Boolean)

object SuccessResponse:
  given Decoder[SuccessResponse] = Decoder.instance { c =>
    c.downField("success")
      .as[Option[Boolean]]
      .map(b => SuccessResponse(b.getOrElse(false)))
  }
