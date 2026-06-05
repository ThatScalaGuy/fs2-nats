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

package fs2.nats.objectstore

import com.github.plokhotnyuk.jsoniter_scala.core.*
import fs2.nats.objectstore.protocol.{WireObjectInfo, WireOptions}

class ObjModelsSpec extends munit.FunSuite:

  test("WireObjectInfo round-trips a full object"):
    val w = WireObjectInfo(
      name = "report.pdf",
      bucket = "docs",
      nuid = "ABCDEF",
      size = 12345L,
      chunks = 3L,
      digest = "SHA-256=Zm9vYmFy",
      description = Some("quarterly report"),
      metadata = Map("content-type" -> "application/pdf", "owner" -> "sven"),
      deleted = false,
      options = Some(WireOptions(maxChunkSize = Some(131072)))
    )
    val json = writeToString(w)
    assert(json.contains("\"max_chunk_size\":131072"), clue = json)
    assert(json.contains("\"digest\":\"SHA-256=Zm9vYmFy\""), clue = json)
    assertEquals(readFromString[WireObjectInfo](json), w)

  test("WireObjectInfo encodes a link under options"):
    val w = WireObjectInfo(
      name = "alias",
      bucket = "docs",
      nuid = "LINKNUID",
      options =
        Some(WireOptions(link = Some(ObjectLink("other", Some("real")))))
    )
    val json = writeToString(w)
    assert(json.contains("\"link\""), clue = json)
    assert(json.contains("\"bucket\":\"other\""), clue = json)
    assert(json.contains("\"name\":\"real\""), clue = json)
    assertEquals(readFromString[WireObjectInfo](json), w)

  test("WireObjectInfo omits empty/default fields (deleted tombstone)"):
    val w = WireObjectInfo(
      name = "gone",
      bucket = "docs",
      nuid = "N",
      size = 0L,
      chunks = 0L,
      digest = "",
      deleted = true
    )
    val json = writeToString(w)
    assert(json.contains("\"deleted\":true"), clue = json)
    assert(!json.contains("description"), clue = json)
    assert(!json.contains("metadata"), clue = json)
    assert(!json.contains("options"), clue = json)
    assertEquals(readFromString[WireObjectInfo](json), w)
