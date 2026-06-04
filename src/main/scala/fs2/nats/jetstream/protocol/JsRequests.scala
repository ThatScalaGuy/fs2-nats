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

/** Request bodies for the JetStream management API, replacing the ad-hoc
  * `Json.obj` builders. All omit their `None`/default fields on the wire.
  */

/** Offset cursor for paginated list endpoints. */
private[jetstream] final case class OffsetRequest(offset: Int)
private[jetstream] object OffsetRequest:
  given JsonValueCodec[OffsetRequest] = JsonCodecMaker.make(JsWire.snake)

/** `STREAM.PURGE` request. */
private[jetstream] final case class PurgeRequest(
    filter: Option[String] = None,
    seq: Option[Long] = None,
    keep: Option[Long] = None
)
private[jetstream] object PurgeRequest:
  given JsonValueCodec[PurgeRequest] = JsonCodecMaker.make(JsWire.snake)

/** `STREAM.MSG.GET` request: by sequence or last-by-subject. */
private[jetstream] final case class MsgGetRequest(
    seq: Option[Long] = None,
    lastBySubj: Option[String] = None
)
private[jetstream] object MsgGetRequest:
  given JsonValueCodec[MsgGetRequest] = JsonCodecMaker.make(JsWire.snake)

/** `STREAM.MSG.DELETE` request; `no_erase` is emitted only when true. */
private[jetstream] final case class DeleteMsgRequest(
    seq: Long,
    noErase: Boolean = false
)
private[jetstream] object DeleteMsgRequest:
  given JsonValueCodec[DeleteMsgRequest] = JsonCodecMaker.make(JsWire.snake)

/** `CONSUMER.CREATE`/`DURABLE.CREATE` request envelope. */
private[jetstream] final case class ConsumerCreateRequest(
    streamName: String,
    config: ConsumerConfig,
    action: String
)
private[jetstream] object ConsumerCreateRequest:
  given JsonValueCodec[ConsumerCreateRequest] =
    JsonCodecMaker.make(JsWire.snake)
