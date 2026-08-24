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
    val (statusCode, statusDescription, headers) = result.toOption.get
    assertEquals(statusCode, Some(503))
    assertEquals(statusDescription, None)
    assertEquals(headers.isEmpty, true)
  }

  test("parseWithStatus with no status code") {
    val input = "NATS/1.0\r\nX-Test: value\r\n\r\n"
    val result = Headers.parseWithStatus(input)

    assert(result.isRight)
    val (statusCode, statusDescription, headers) = result.toOption.get
    assertEquals(statusCode, None)
    assertEquals(statusDescription, None)
    assertEquals(headers.get("X-Test"), Some("value"))
  }

  test("parseWithStatus with status 404") {
    val input = "NATS/1.0 404\r\nDescription: Not Found\r\n\r\n"
    val result = Headers.parseWithStatus(input)

    assert(result.isRight)
    val (statusCode, _, headers) = result.toOption.get
    assertEquals(statusCode, Some(404))
    assertEquals(headers.get("Description"), Some("Not Found"))
  }

  test("parseWithStatus splits code and description on the version line") {
    val input = "NATS/1.0 100 Idle Heartbeat\r\nNats-Last-Consumer: 5\r\n\r\n"
    val result = Headers.parseWithStatus(input)

    assert(result.isRight)
    val (statusCode, statusDescription, headers) = result.toOption.get
    assertEquals(statusCode, Some(100))
    assertEquals(statusDescription, Some("Idle Heartbeat"))
    assertEquals(headers.get("Nats-Last-Consumer"), Some("5"))
  }

  test("parseWithStatus parses multi-word description (No Messages)") {
    val input = "NATS/1.0 404 No Messages\r\n\r\n"
    val result = Headers.parseWithStatus(input)

    assert(result.isRight)
    val (statusCode, statusDescription, _) = result.toOption.get
    assertEquals(statusCode, Some(404))
    assertEquals(statusDescription, Some("No Messages"))
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

  private val V = "Invalid NATS headers: missing or invalid version line"
  private def R(c: Option[Int], d: Option[String], h: Headers) =
    Right((c, d, h))

  // Pins every semantic of the header-block parser that a caller can observe,
  // asserted on the whole `Either` so a rewrite cannot drift in the status
  // tuple, the entry order or a single character of the error text. The odd
  // rows are deliberate: a lone CR or LF does not terminate a line, everything
  // after the first blank line is dropped, `trim` strips C0 controls but not
  // NBSP, and the status split uses the wider `Character.isWhitespace` set.
  test("parseWithStatus acceptance matrix") {
    val cases: List[
      (String, Either[String, (Option[Int], Option[String], Headers)])
    ] =
      List(
        "" -> Left(V),
        "NATS/1.0" -> R(None, None, Headers.empty),
        "NATS/1.0\r\n" -> R(None, None, Headers.empty),
        "NATS/1.0\r\n\r\n" -> R(None, None, Headers.empty),
        "NATS/1.0 503\r\n\r\n" -> R(Some(503), None, Headers.empty),
        "NATS/1.0 100 Idle Heartbeat\r\n\r\n" ->
          R(Some(100), Some("Idle Heartbeat"), Headers.empty),
        "NATS/1.0  100  Idle  Heartbeat \r\n\r\n" ->
          R(Some(100), Some("Idle  Heartbeat"), Headers.empty),
        "NATS/1.0 abc\r\n\r\n" -> R(None, None, Headers.empty),
        "NATS/1.0 -1\r\n\r\n" -> R(Some(-1), None, Headers.empty),
        "NATS/1.0 0100\r\n\r\n" -> R(Some(100), None, Headers.empty),
        "NATS/1.0 99999999999\r\n\r\n" -> R(None, None, Headers.empty),
        "NATS/1.0 abc def\r\n\r\n" -> R(None, Some("def"), Headers.empty),
        "NATS/1.0 404 No Messages\r\n\r\n" ->
          R(Some(404), Some("No Messages"), Headers.empty),
        "NATS/1.0 100 \r\n\r\n" -> R(Some(100), None, Headers.empty),
        "NATS/1.0X\r\n\r\n" -> R(None, None, Headers.empty),
        "NATS/1.00 200\r\n\r\n" -> R(Some(0), Some("200"), Headers.empty),
        "nats/1.0\r\n\r\n" -> Left(V),
        "NATS/1.0 \r\n\r\n" -> R(None, None, Headers.empty),
        "NATS/1.0\t100\r\n\r\n" -> R(Some(100), None, Headers.empty),
        "NATS/1.0 100\u2003Idle\r\n\r\n" ->
          R(Some(100), Some("Idle"), Headers.empty),
        "NATS/1.0 \u2003100 Idle\r\n\r\n" ->
          R(None, Some("100 Idle"), Headers.empty),
        "NATS/1.0\rA: b\r\n\r\n" -> R(None, Some("b"), Headers.empty),
        "NATS/1.0\nA: b\r\n\r\n" -> R(None, Some("b"), Headers.empty),
        "NATS/1.0\r\nA: b\r\n\r\nC: d\r\n\r\n" ->
          R(None, None, Headers("A" -> "b")),
        "NATS/1.0\r\n\r\nA: b\r\n\r\n" -> R(None, None, Headers.empty),
        "NATS/1.0\r\nA: b" -> R(None, None, Headers("A" -> "b")),
        "NATS/1.0\r\nBad1\r\nOk: 1\r\nBad2\r\n\r\n" -> Left(
          "Invalid header line (no colon): Bad1; " +
            "Invalid header line (no colon): Bad2"
        ),
        "NATS/1.0\r\n \r\n\r\n" -> Left("Invalid header line (no colon):  "),
        "NATS/1.0\r\n: v\r\n\r\n" -> R(None, None, Headers("" -> "v")),
        "NATS/1.0\r\na: b: c\r\n\r\n" -> R(None, None, Headers("a" -> "b: c")),
        "NATS/1.0\r\na:    \r\n\r\n" -> R(None, None, Headers("a" -> "")),
        "NATS/1.0\r\n  a  :  b  \r\n\r\n" -> R(None, None, Headers("a" -> "b")),
        "NATS/1.0\r\nA: b\r\nA: c\r\n\r\n" ->
          R(None, None, Headers("A" -> "b", "A" -> "c")),
        "NATS/1.0\r\nA: b\nc\r\n\r\n" -> R(None, None, Headers("A" -> "b\nc")),
        "NATS/1.0\r\nA: b\r\r\n\r\n" -> R(None, None, Headers("A" -> "b")),
        "NATS/1.0\r\nKéy: välüe\r\n\r\n" ->
          R(None, None, Headers("Kéy" -> "välüe")),
        "NATS/1.0\r\nA: \u00a0b\u00a0\r\n\r\n" ->
          R(None, None, Headers("A" -> "\u00a0b\u00a0")),
        "NATS/1.0\r\nA: \u0001b\u0001\r\n\r\n" ->
          R(None, None, Headers("A" -> "b"))
      )
    cases.foreach { case (in, expected) =>
      assertEquals(Headers.parseWithStatus(in), expected, in)
      assertEquals(Headers.parse(in), expected.map(_._3), in)
    }
  }

  test("parse decodes malformed UTF-8 to the replacement character") {
    // "NATS/1.0\r\nA: <C3>\r\n\r\n" - a truncated 2-byte lead.
    val raw = Array[Byte](
      0x4e,
      0x41,
      0x54,
      0x53,
      0x2f,
      0x31,
      0x2e,
      0x30,
      0x0d,
      0x0a,
      0x41,
      0x3a,
      0x20,
      0xc3.toByte,
      0x0d,
      0x0a,
      0x0d,
      0x0a
    )
    assertEquals(
      Headers.parse(Chunk.array(raw)),
      Right(Headers("A" -> "\ufffd"))
    )
  }

  test("parse reads a non-zero-offset Chunk slice") {
    val pad = "xxxx".getBytes(StandardCharsets.UTF_8)
    val body = "NATS/1.0\r\nA: b\r\n\r\n".getBytes(StandardCharsets.UTF_8)
    val c = Chunk.array(pad ++ body).drop(pad.length)
    assertEquals(Headers.parse(c), Right(Headers("A" -> "b")))
  }
