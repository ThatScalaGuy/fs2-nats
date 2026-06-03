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

import cats.effect.Sync

/** Utilities for generating NATS inbox identifiers and request-correlation
  * tokens.
  *
  * The request/reply primitive uses a single shared inbox subscription
  * `_INBOX.<inboxId>.*` per connection plus a unique per-request token so
  * replies can be correlated back to the originating request.
  */
object Tokens:

  private val Base62Chars: Array[Char] =
    ("0123456789" +
      "ABCDEFGHIJKLMNOPQRSTUVWXYZ" +
      "abcdefghijklmnopqrstuvwxyz").toCharArray

  /** Length of a generated inbox identifier (mirrors the NATS NUID size). */
  val InboxIdLength: Int = 22

  /** Generate a random, connection-unique inbox identifier consisting of base62
    * characters. Uses a securely-seeded source so identifiers do not collide
    * across clients/processes sharing a server.
    *
    * @param length
    *   The number of characters to generate (default [[InboxIdLength]])
    * @return
    *   An effect producing the random identifier
    */
  def randomInboxId[F[_]: Sync](length: Int = InboxIdLength): F[String] =
    Sync[F].delay {
      val rng = new java.security.SecureRandom()
      val sb = new java.lang.StringBuilder(length)
      var i = 0
      while i < length do
        sb.append(Base62Chars(rng.nextInt(Base62Chars.length)))
        i += 1
      sb.toString
    }

  /** Encode a non-negative `Long` as a base62 string. Used for per-request
    * correlation tokens drawn from a monotonic counter, which guarantees the
    * token is unique among in-flight requests (and never reused) without any
    * collision risk.
    *
    * @param value
    *   A non-negative value to encode
    * @return
    *   The base62 representation (e.g. `0` -> "0", `61` -> "z", `62` -> "10")
    */
  def base62(value: Long): String =
    require(value >= 0, s"base62 expects a non-negative value, got $value")
    if value == 0 then "0"
    else
      val sb = new java.lang.StringBuilder()
      var n = value
      while n > 0 do
        sb.append(Base62Chars((n % 62).toInt))
        n = n / 62
      sb.reverse().toString
