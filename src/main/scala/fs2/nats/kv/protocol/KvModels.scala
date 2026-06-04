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

package fs2.nats.kv.protocol

import com.github.plokhotnyuk.jsoniter_scala.core.*

/** Request body for `$JS.API.DIRECT.GET.<stream>`: select a stored message by
  * sequence or by last-for-subject. Request-only — the reply is the raw stored
  * message (payload + server headers), not a JSON envelope.
  */
private[kv] final case class DirectGetRequest(
    seq: Option[Long] = None,
    lastBySubj: Option[String] = None
)

private[kv] object DirectGetRequest:
  given JsonValueCodec[DirectGetRequest] =
    new JsonValueCodec[DirectGetRequest]:
      def nullValue: DirectGetRequest = null
      def decodeValue(
          in: JsonReader,
          default: DirectGetRequest
      ): DirectGetRequest =
        in.decodeError("DirectGetRequest is encode-only")
      def encodeValue(x: DirectGetRequest, out: JsonWriter): Unit =
        out.writeObjectStart()
        x.seq.foreach { v => out.writeKey("seq"); out.writeVal(v) }
        x.lastBySubj.foreach { v =>
          out.writeKey("last_by_subj"); out.writeVal(v)
        }
        out.writeObjectEnd()
