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

package fs2.nats.objectstore.protocol

import com.github.plokhotnyuk.jsoniter_scala.core.*
import com.github.plokhotnyuk.jsoniter_scala.macros.*
import fs2.nats.objectstore.ObjectLink

/** Wire (JSON) model for the stored object meta published to
  * `$O.<bucket>.M.<name>`. Mirrors the NATS Object Store `ObjectInfo` payload;
  * the client-side `mtime` is taken from the message timestamp and is therefore
  * not part of the wire model. jsoniter's defaults omit `None`/empty/default
  * fields, matching the reference clients' output.
  */
private[objectstore] object ObjWire:
  inline def snake: CodecMakerConfig =
    CodecMakerConfig.withFieldNameMapper(JsonCodecMaker.enforce_snake_case)

/** The optional `options` block: an embedded link and/or the object's
  * configured max chunk size.
  */
private[objectstore] final case class WireOptions(
    link: Option[ObjectLink] = None,
    maxChunkSize: Option[Int] = None
)
private[objectstore] object WireOptions:
  given JsonValueCodec[WireOptions] = JsonCodecMaker.make(ObjWire.snake)

/** The stored object meta payload. */
private[objectstore] final case class WireObjectInfo(
    name: String,
    bucket: String = "",
    nuid: String = "",
    size: Long = 0L,
    chunks: Long = 0L,
    digest: String = "",
    description: Option[String] = None,
    metadata: Map[String, String] = Map.empty,
    deleted: Boolean = false,
    options: Option[WireOptions] = None
)
private[objectstore] object WireObjectInfo:
  given JsonValueCodec[WireObjectInfo] = JsonCodecMaker.make(ObjWire.snake)
