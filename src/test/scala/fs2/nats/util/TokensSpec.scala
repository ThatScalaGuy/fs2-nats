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

package fs2.nats.util

import cats.effect.IO
import cats.syntax.all.*
import munit.CatsEffectSuite

class TokensSpec extends CatsEffectSuite:

  private val base62Pattern = "^[0-9A-Za-z]+$".r

  test("randomInboxId has the requested length and is base62") {
    Tokens.randomInboxId[IO]().map { id =>
      assertEquals(id.length, Tokens.InboxIdLength)
      assert(
        base62Pattern.matches(id),
        s"inbox id '$id' is not base62"
      )
    }
  }

  test("randomInboxId honors a custom length") {
    Tokens.randomInboxId[IO](8).map(id => assertEquals(id.length, 8))
  }

  test("randomInboxId produces unique ids") {
    List.fill(100)(Tokens.randomInboxId[IO]()).sequence.map { ids =>
      assertEquals(ids.toSet.size, ids.size)
    }
  }

  test("base62 encodes small values") {
    assertEquals(Tokens.base62(0L), "0")
    assertEquals(Tokens.base62(1L), "1")
    assertEquals(Tokens.base62(61L), "z")
    assertEquals(Tokens.base62(62L), "10")
  }

  test("base62 produces distinct base62 tokens for a counter range") {
    val tokens = (0L until 1000L).map(Tokens.base62)
    assertEquals(tokens.toSet.size, 1000)
    tokens.foreach { t =>
      assert(base62Pattern.matches(t), s"token '$t' is not base62")
    }
  }

  test("base62 rejects negative values") {
    intercept[IllegalArgumentException](Tokens.base62(-1L))
  }
