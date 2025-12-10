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

package fs2.nats.transport

import cats.effect.IO
import munit.CatsEffectSuite
import fs2.Chunk
import fs2.nats.publish.SerializationUtils

class SerializationUtilsSpec extends CatsEffectSuite:

  test("buildPub without reply-to") {
    val result = SerializationUtils.buildPub(
      "FOO.BAR",
      None,
      Chunk.array("Hello".getBytes)
    )
    val expected = "PUB FOO.BAR 5\r\nHello\r\n"
    assertEquals(new String(result.toArray), expected)
  }

  test("buildPub with reply-to") {
    val result = SerializationUtils.buildPub(
      "FOO",
      Some("INBOX.123"),
      Chunk.array("Hi".getBytes)
    )
    val expected = "PUB FOO INBOX.123 2\r\nHi\r\n"
    assertEquals(new String(result.toArray), expected)
  }

  test("buildPub with empty payload") {
    val result = SerializationUtils.buildPub("FOO", None, Chunk.empty)
    val expected = "PUB FOO 0\r\n\r\n"
    assertEquals(new String(result.toArray), expected)
  }

  test("buildHPub without reply-to") {
    val headers = Chunk.array("NATS/1.0\r\nX-Test: value\r\n\r\n".getBytes)
    val payload = Chunk.array("data".getBytes)
    val result = SerializationUtils.buildHPub("FOO", None, headers, payload)

    val headerLen = headers.size
    val totalLen = headerLen + payload.size
    val controlLine = s"HPUB FOO $headerLen $totalLen\r\n"

    assert(new String(result.toArray).startsWith(controlLine))
  }

  test("buildHPub with reply-to") {
    val headers = Chunk.array("NATS/1.0\r\n\r\n".getBytes)
    val payload = Chunk.array("test".getBytes)
    val result =
      SerializationUtils.buildHPub("FOO", Some("INBOX.X"), headers, payload)

    val expected = "HPUB FOO INBOX.X"
    assert(new String(result.toArray).startsWith(expected))
  }

  test("buildSub without queue group") {
    val result = SerializationUtils.buildSub("FOO.>", None, 42)
    val expected = "SUB FOO.> 42\r\n"
    assertEquals(new String(result.toArray), expected)
  }

  test("buildSub with queue group") {
    val result = SerializationUtils.buildSub("FOO.*", Some("workers"), 1)
    val expected = "SUB FOO.* workers 1\r\n"
    assertEquals(new String(result.toArray), expected)
  }

  test("buildUnsub without max msgs") {
    val result = SerializationUtils.buildUnsub(42, None)
    val expected = "UNSUB 42\r\n"
    assertEquals(new String(result.toArray), expected)
  }

  test("buildUnsub with max msgs") {
    val result = SerializationUtils.buildUnsub(42, Some(10))
    val expected = "UNSUB 42 10\r\n"
    assertEquals(new String(result.toArray), expected)
  }

  test("buildConnect") {
    val json = """{"verbose":false}"""
    val result = SerializationUtils.buildConnect(json)
    val expected = s"CONNECT $json\r\n"
    assertEquals(new String(result.toArray), expected)
  }

  test("buildPing") {
    assertEquals(new String(SerializationUtils.buildPing.toArray), "PING\r\n")
  }

  test("buildPong") {
    assertEquals(new String(SerializationUtils.buildPong.toArray), "PONG\r\n")
  }

  test("validateSubject accepts valid subjects") {
    assert(SerializationUtils.validateSubject("FOO").isRight)
    assert(SerializationUtils.validateSubject("FOO.BAR").isRight)
    assert(SerializationUtils.validateSubject("FOO.BAR.BAZ").isRight)
    assert(SerializationUtils.validateSubject("FOO.*").isRight)
    assert(SerializationUtils.validateSubject("FOO.>").isRight)
  }

  test("validateSubject rejects empty subject") {
    assert(SerializationUtils.validateSubject("").isLeft)
  }

  test("validateSubject rejects subject with spaces") {
    assert(SerializationUtils.validateSubject("FOO BAR").isLeft)
  }

  test("validateSubject rejects subject starting with dot") {
    assert(SerializationUtils.validateSubject(".FOO").isLeft)
  }

  test("validateSubject rejects subject ending with dot") {
    assert(SerializationUtils.validateSubject("FOO.").isLeft)
  }

  test("validateSubject rejects subject with double dots") {
    assert(SerializationUtils.validateSubject("FOO..BAR").isLeft)
  }

  test("byteLength calculates UTF-8 length correctly") {
    assertEquals(SerializationUtils.byteLength("Hello"), 5)
    assertEquals(
      SerializationUtils.byteLength("Héllo"),
      6
    ) // é is 2 bytes in UTF-8
    assertEquals(
      SerializationUtils.byteLength("你好"),
      6
    ) // Chinese chars are 3 bytes each
  }
