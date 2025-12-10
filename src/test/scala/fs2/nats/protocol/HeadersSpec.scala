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

package fs2.nats.protocol

import cats.effect.IO
import fs2.Chunk
import munit.CatsEffectSuite
import java.nio.charset.StandardCharsets

class HeadersSpec extends CatsEffectSuite:

  test("empty headers") {
    val headers = Headers.empty
    assertEquals(headers.isEmpty, true)
    assertEquals(headers.size, 0)
    assertEquals(headers.get("X-Test"), None)
  }

  test("create headers from varargs") {
    val headers = Headers("X-One" -> "1", "X-Two" -> "2")
    assertEquals(headers.size, 2)
    assertEquals(headers.get("X-One"), Some("1"))
    assertEquals(headers.get("X-Two"), Some("2"))
  }

  test("case-insensitive lookup") {
    val headers = Headers("X-Custom-Header" -> "value")
    assertEquals(headers.get("x-custom-header"), Some("value"))
    assertEquals(headers.get("X-CUSTOM-HEADER"), Some("value"))
    assertEquals(headers.get("X-Custom-Header"), Some("value"))
  }

  test("add header preserves existing") {
    val headers = Headers("X-One" -> "1")
      .add("X-One", "2")
      .add("X-Two", "value")

    assertEquals(headers.size, 3)
    assertEquals(headers.getAll("X-One").toList, List("1", "2"))
    assertEquals(headers.get("X-Two"), Some("value"))
  }

  test("set header replaces existing") {
    val headers = Headers("X-One" -> "1", "X-One" -> "2")
      .set("X-One", "replaced")

    assertEquals(headers.getAll("X-One").toList, List("replaced"))
  }

  test("remove header") {
    val headers = Headers("X-One" -> "1", "X-Two" -> "2")
      .remove("X-One")

    assertEquals(headers.get("X-One"), None)
    assertEquals(headers.get("X-Two"), Some("2"))
  }

  test("contains check") {
    val headers = Headers("X-Test" -> "value")
    assertEquals(headers.contains("X-Test"), true)
    assertEquals(headers.contains("x-test"), true)
    assertEquals(headers.contains("X-Other"), false)
  }

  test("toBytes serialization") {
    val headers = Headers("X-One" -> "1", "X-Two" -> "2")
    val bytes = headers.toBytes
    val str = new String(bytes.toArray, StandardCharsets.UTF_8)

    assert(str.startsWith("NATS/1.0\r\n"))
    assert(str.contains("X-One: 1\r\n"))
    assert(str.contains("X-Two: 2\r\n"))
    assert(str.endsWith("\r\n\r\n"))
  }

  test("empty headers toBytes returns empty chunk") {
    assertEquals(Headers.empty.toBytes, Chunk.empty)
  }

  test("parse headers from string") {
    val input = "NATS/1.0\r\nX-Test: value\r\nX-Other: foo\r\n\r\n"
    val result = Headers.parse(input)

    assert(result.isRight)
    val headers = result.toOption.get
    assertEquals(headers.get("X-Test"), Some("value"))
    assertEquals(headers.get("X-Other"), Some("foo"))
  }

  test("parse headers from bytes") {
    val input = Chunk.array(
      "NATS/1.0\r\nFoo: bar\r\n\r\n".getBytes(StandardCharsets.UTF_8)
    )
    val result = Headers.parse(input)

    assert(result.isRight)
    assertEquals(result.toOption.get.get("Foo"), Some("bar"))
  }

  test("parse fails on missing version line") {
    val input = "X-Test: value\r\n\r\n"
    val result = Headers.parse(input)
    assert(result.isLeft)
  }

  test("parse fails on malformed header line") {
    val input = "NATS/1.0\r\nBadLine\r\n\r\n"
    val result = Headers.parse(input)
    assert(result.isLeft)
    assert(result.left.toOption.get.contains("no colon"))
  }

  test("parseWithStatus extracts status code") {
    val input = "NATS/1.0 503\r\n\r\n"
    val result = Headers.parseWithStatus(input)

    assert(result.isRight)
    val (statusCode, headers) = result.toOption.get
    assertEquals(statusCode, Some(503))
    assertEquals(headers.isEmpty, true)
  }

  test("parseWithStatus with no status code") {
    val input = "NATS/1.0\r\nX-Test: value\r\n\r\n"
    val result = Headers.parseWithStatus(input)

    assert(result.isRight)
    val (statusCode, headers) = result.toOption.get
    assertEquals(statusCode, None)
    assertEquals(headers.get("X-Test"), Some("value"))
  }

  test("parseWithStatus with status 404") {
    val input = "NATS/1.0 404\r\nDescription: Not Found\r\n\r\n"
    val result = Headers.parseWithStatus(input)

    assert(result.isRight)
    val (statusCode, headers) = result.toOption.get
    assertEquals(statusCode, Some(404))
    assertEquals(headers.get("Description"), Some("Not Found"))
  }

  test("byteLength calculation") {
    val headers = Headers("X-One" -> "1")
    val bytes = headers.toBytes
    assertEquals(headers.byteLength, bytes.size)
  }

  test("fromMap creation") {
    val headers = Headers.fromMap(Map("A" -> "1", "B" -> "2"))
    assertEquals(headers.size, 2)
    assertEquals(headers.get("A"), Some("1"))
    assertEquals(headers.get("B"), Some("2"))
  }

  test("preserve insertion order") {
    val headers = Headers(
      "Z-Header" -> "1",
      "A-Header" -> "2",
      "M-Header" -> "3"
    )
    val entries = headers.entries
    assertEquals(entries(0)._1, "Z-Header")
    assertEquals(entries(1)._1, "A-Header")
    assertEquals(entries(2)._1, "M-Header")
  }

  test("multi-value headers with getAll") {
    val headers = Headers(
      "Accept" -> "text/plain",
      "Accept" -> "application/json",
      "Accept" -> "text/html"
    )
    val values = headers.getAll("Accept")
    assertEquals(values.size, 3)
    assertEquals(
      values.toList,
      List("text/plain", "application/json", "text/html")
    )
  }

  test("get returns first value for multi-value header") {
    val headers = Headers(
      "X-Multi" -> "first",
      "X-Multi" -> "second"
    )
    assertEquals(headers.get("X-Multi"), Some("first"))
  }

  test("header values preserve whitespace") {
    val headers = Headers("X-Test" -> "  spaced  value  ")
    assertEquals(headers.get("X-Test"), Some("  spaced  value  "))
  }
