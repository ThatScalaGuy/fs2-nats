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

package fs2.nats.kv

import munit.FunSuite

class KvNamesSpec extends FunSuite:

  test("stream name and bucket round-trip") {
    assertEquals(KvNames.streamName("config"), "KV_config")
    assertEquals(KvNames.bucketFromStream("KV_config"), Some("config"))
    assertEquals(KvNames.bucketFromStream("ORDERS"), None)
  }

  test("key <-> subject mapping") {
    assertEquals(KvNames.keySubject("config", "a.b"), "$KV.config.a.b")
    assertEquals(KvNames.allKeysSubject("config"), "$KV.config.>")
    assertEquals(KvNames.patternSubject("config", "a.*"), "$KV.config.a.*")
    assertEquals(KvNames.keyFromSubject("config", "$KV.config.a.b"), "a.b")
  }

  test("bucket validation") {
    assert(KvNames.validateBucket("my-bucket_1").isRight)
    assert(KvNames.validateBucket("").isLeft)
    assert(KvNames.validateBucket("bad bucket").isLeft)
    assert(KvNames.validateBucket("bad.bucket").isLeft)
    assert(KvNames.validateBucket("bad>").isLeft)
  }

  test("key validation rejects wildcards and bad dots") {
    assert(KvNames.validateKey("a.b.c").isRight)
    assert(KvNames.validateKey("a-b/c=d_e").isRight)
    assert(KvNames.validateKey("").isLeft)
    assert(KvNames.validateKey(".leading").isLeft)
    assert(KvNames.validateKey("trailing.").isLeft)
    assert(KvNames.validateKey("a b").isLeft)
    assert(KvNames.validateKey("a.*").isLeft)
    assert(KvNames.validateKey("a.>").isLeft)
  }

  test("key pattern validation allows wildcards") {
    assert(KvNames.validateKeyPattern("a.*").isRight)
    assert(KvNames.validateKeyPattern("a.>").isRight)
    assert(KvNames.validateKeyPattern(">").isRight)
    assert(KvNames.validateKeyPattern("a b").isLeft)
    assert(KvNames.validateKeyPattern("trailing.").isLeft)
  }
