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

import fs2.nats.jetstream.protocol.DiscardPolicy
import munit.FunSuite

import scala.concurrent.duration.*

class KvConfigSpec extends FunSuite:

  test("toStreamConfig maps KV semantics onto a stream") {
    val sc = KvConfig(
      bucket = "config",
      history = 5,
      ttl = Some(1.hour),
      maxValueSize = 4096,
      maxBytes = 1048576L
    ).toStreamConfig

    assertEquals(sc.name, "KV_config")
    assertEquals(sc.subjects, List("$KV.config.>"))
    assertEquals(sc.maxMsgsPerSubject, 5L)
    assertEquals(sc.maxAge, Some(1.hour))
    assertEquals(sc.maxMsgSize, 4096)
    assertEquals(sc.maxBytes, 1048576L)
    assertEquals(sc.discard, DiscardPolicy.New)
    assert(sc.allowRollupHdrs)
    assert(sc.denyDelete)
    assert(!sc.discardNewPerSubject)
    assert(sc.allowDirect)
  }

  test("default history is 1") {
    assertEquals(KvConfig("config").toStreamConfig.maxMsgsPerSubject, 1L)
  }

  test("validate accepts good config and rejects bad bucket/history") {
    assert(KvConfig("config").validate.isRight)
    assert(KvConfig("bad bucket").validate.isLeft)
    assert(KvConfig("config", history = 0).validate.isLeft)
    assert(KvConfig("config", history = 65).validate.isLeft)
    assert(KvConfig("config", history = 64).validate.isRight)
  }
