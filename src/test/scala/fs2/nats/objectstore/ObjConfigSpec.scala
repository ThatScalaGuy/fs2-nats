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

import fs2.nats.jetstream.protocol.{DiscardPolicy, StoreCompression}

class ObjConfigSpec extends munit.FunSuite:

  test("toStreamConfig sets the two subjects, discard=new, rollup, direct, S2"):
    val sc = ObjConfig("photos").toStreamConfig
    assertEquals(sc.name, "OBJ_photos")
    assertEquals(
      sc.subjects,
      List("$O.photos.C.>", "$O.photos.M.>")
    )
    assertEquals(sc.discard, DiscardPolicy.New)
    assert(sc.allowRollupHdrs, "allowRollupHdrs must be set for meta rollup")
    assert(sc.allowDirect, "allowDirect default on")
    assert(
      !sc.denyDelete,
      "object store purges chunks, so deny_delete must be off"
    )
    assertEquals(sc.compression, StoreCompression.S2)

  test("validate rejects a bad bucket name"):
    assert(ObjConfig("bad.name").validate.isLeft)
    assert(ObjConfig("good_name-1").validate.isRight)

  test("DefaultChunkSize is 128 KiB"):
    assertEquals(ObjConfig.DefaultChunkSize, 131072)
