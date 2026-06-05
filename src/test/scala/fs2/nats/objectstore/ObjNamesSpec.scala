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

class ObjNamesSpec extends munit.FunSuite:

  test("stream name prefixes/strips OBJ_"):
    assertEquals(ObjNames.streamName("photos"), "OBJ_photos")
    assertEquals(ObjNames.bucketFromStream("OBJ_photos"), Some("photos"))
    assertEquals(ObjNames.bucketFromStream("KV_x"), None)

  test("chunk and meta subjects"):
    assertEquals(ObjNames.chunkStreamSubject("b"), "$O.b.C.>")
    assertEquals(ObjNames.metaStreamSubject("b"), "$O.b.M.>")
    assertEquals(ObjNames.chunkSubject("b", "NUID123"), "$O.b.C.NUID123")

  test("object name <-> meta subject round-trips via url-safe base64"):
    val names = List(
      "simple",
      "with/slashes",
      "with spaces and.dots",
      "weird:chars*>?",
      "unicode-café-éè",
      "a.b.c.d"
    )
    names.foreach { name =>
      val subj = ObjNames.metaSubject("b", name)
      assert(subj.startsWith("$O.b.M."), clue = subj)
      // the encoded token must be a single subject token (no '.', '*', '>', ' ')
      val token = subj.stripPrefix("$O.b.M.")
      assert(!token.contains('.'), clue = token)
      assert(!token.contains(' '), clue = token)
      assert(!token.contains('*'), clue = token)
      assert(!token.contains('>'), clue = token)
      assertEquals(ObjNames.nameFromMetaSubject("b", subj), name)
    }

  test("bucket validation"):
    assert(ObjNames.validateBucket("photos-1_A").isRight)
    assert(ObjNames.validateBucket("").isLeft)
    assert(ObjNames.validateBucket("has.dot").isLeft)
    assert(ObjNames.validateBucket("has space").isLeft)

  test("object name validation"):
    assert(ObjNames.validateObjectName("a/b.txt").isRight)
    assert(ObjNames.validateObjectName("").isLeft)
    assert(
      ObjNames.validateObjectName("x" * (ObjNames.MaxNameBytes + 1)).isLeft
    )
