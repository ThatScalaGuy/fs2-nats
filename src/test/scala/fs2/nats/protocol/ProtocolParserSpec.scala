/*
 * Copyright 2024 fs2-nats contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package fs2.nats.protocol

import cats.effect.IO
import fs2.{Chunk, Stream}
import munit.CatsEffectSuite
import java.nio.charset.StandardCharsets

class ProtocolParserSpec extends CatsEffectSuite:

  private def parseBytes(bytes: Array[Byte]): IO[List[NatsFrame]] =
    Stream.emit(Chunk.array(bytes))
      .unchunks
      .through(ProtocolParser.parseStream[IO])
      .compile
      .toList

  private def parseChunks(chunks: List[Chunk[Byte]]): IO[List[NatsFrame]] =
    Stream.emits(chunks)
      .unchunks
      .through(ProtocolParser.parseStream[IO])
      .compile
      .toList

  private def parseString(s: String): IO[List[NatsFrame]] =
    parseBytes(s.getBytes(StandardCharsets.UTF_8))

  private def toBytes(s: String): Chunk[Byte] =
    Chunk.array(s.getBytes(StandardCharsets.UTF_8))

  // --- PING/PONG Tests ---

  test("parse PING") {
    parseString("PING\r\n").map { frames =>
      assertEquals(frames, List(NatsFrame.PingFrame))
    }
  }

  test("parse PONG") {
    parseString("PONG\r\n").map { frames =>
      assertEquals(frames, List(NatsFrame.PongFrame))
    }
  }

  test("parse multiple PING/PONG") {
    parseString("PING\r\nPONG\r\nPING\r\n").map { frames =>
      assertEquals(frames, List(NatsFrame.PingFrame, NatsFrame.PongFrame, NatsFrame.PingFrame))
    }
  }

  // --- OK/ERR Tests ---

  test("parse +OK") {
    parseString("+OK\r\n").map { frames =>
      assertEquals(frames, List(NatsFrame.OkFrame))
    }
  }

  test("parse -ERR with message") {
    parseString("-ERR 'Authorization Violation'\r\n").map { frames =>
      assertEquals(frames, List(NatsFrame.ErrFrame("Authorization Violation")))
    }
  }

  test("parse -ERR without quotes") {
    parseString("-ERR Unknown Protocol Operation\r\n").map { frames =>
      assertEquals(frames, List(NatsFrame.ErrFrame("Unknown Protocol Operation")))
    }
  }

  // --- INFO Tests ---

  test("parse INFO frame") {
    val infoJson = """{"server_id":"test","version":"2.9.0","proto":1,"go":"go1.19","host":"0.0.0.0","port":4222,"max_payload":1048576}"""
    parseString(s"INFO $infoJson\r\n").map { frames =>
      assertEquals(frames.length, 1)
      frames.head match
        case NatsFrame.InfoFrame(info) =>
          assertEquals(info.serverId, "test")
          assertEquals(info.version, "2.9.0")
          assertEquals(info.proto, 1)
          assertEquals(info.maxPayload, 1048576L)
        case other =>
          fail(s"Expected InfoFrame, got $other")
    }
  }

  test("parse INFO with headers support") {
    val infoJson = """{"server_id":"test","version":"2.9.0","proto":1,"go":"go1.19","host":"0.0.0.0","port":4222,"max_payload":1048576,"headers":true}"""
    parseString(s"INFO $infoJson\r\n").map { frames =>
      frames.head match
        case NatsFrame.InfoFrame(info) =>
          assertEquals(info.headersSupported, true)
        case other =>
          fail(s"Expected InfoFrame, got $other")
    }
  }

  // --- MSG Tests ---

  test("parse MSG without reply-to") {
    parseString("MSG FOO.BAR 1 5\r\nHello\r\n").map { frames =>
      assertEquals(frames.length, 1)
      frames.head match
        case NatsFrame.MsgFrame(subject, sid, replyTo, payload) =>
          assertEquals(subject, "FOO.BAR")
          assertEquals(sid, 1L)
          assertEquals(replyTo, None)
          assertEquals(new String(payload.toArray, StandardCharsets.UTF_8), "Hello")
        case other =>
          fail(s"Expected MsgFrame, got $other")
    }
  }

  test("parse MSG with reply-to") {
    parseString("MSG FOO.BAR 42 INBOX.123 5\r\nHello\r\n").map { frames =>
      assertEquals(frames.length, 1)
      frames.head match
        case NatsFrame.MsgFrame(subject, sid, replyTo, payload) =>
          assertEquals(subject, "FOO.BAR")
          assertEquals(sid, 42L)
          assertEquals(replyTo, Some("INBOX.123"))
          assertEquals(new String(payload.toArray, StandardCharsets.UTF_8), "Hello")
        case other =>
          fail(s"Expected MsgFrame, got $other")
    }
  }

  test("parse MSG with zero-length payload") {
    parseString("MSG FOO 1 0\r\n\r\n").map { frames =>
      assertEquals(frames.length, 1)
      frames.head match
        case NatsFrame.MsgFrame(_, _, _, payload) =>
          assertEquals(payload.size, 0)
        case other =>
          fail(s"Expected MsgFrame, got $other")
    }
  }

  test("parse multiple MSG frames") {
    val data = "MSG FOO 1 5\r\nHello\r\nMSG BAR 2 5\r\nWorld\r\n"
    parseString(data).map { frames =>
      assertEquals(frames.length, 2)
      frames(0) match
        case NatsFrame.MsgFrame(subject, sid, _, payload) =>
          assertEquals(subject, "FOO")
          assertEquals(sid, 1L)
          assertEquals(new String(payload.toArray, StandardCharsets.UTF_8), "Hello")
        case other =>
          fail(s"Expected MsgFrame, got $other")
      frames(1) match
        case NatsFrame.MsgFrame(subject, sid, _, payload) =>
          assertEquals(subject, "BAR")
          assertEquals(sid, 2L)
          assertEquals(new String(payload.toArray, StandardCharsets.UTF_8), "World")
        case other =>
          fail(s"Expected MsgFrame, got $other")
    }
  }

  // --- HMSG Tests ---

  test("parse HMSG with headers") {
    val headers = "NATS/1.0\r\nX-Custom: value\r\n\r\n"
    val payload = "Hello"
    val headerLen = headers.length
    val totalLen = headerLen + payload.length
    val data = s"HMSG FOO 1 $headerLen $totalLen\r\n${headers}${payload}\r\n"

    parseString(data).map { frames =>
      assertEquals(frames.length, 1)
      frames.head match
        case NatsFrame.HMsgFrame(subject, sid, replyTo, hdrs, statusCode, pay) =>
          assertEquals(subject, "FOO")
          assertEquals(sid, 1L)
          assertEquals(replyTo, None)
          assertEquals(hdrs.get("X-Custom"), Some("value"))
          assertEquals(statusCode, None)
          assertEquals(new String(pay.toArray, StandardCharsets.UTF_8), "Hello")
        case other =>
          fail(s"Expected HMsgFrame, got $other")
    }
  }

  test("parse HMSG with reply-to") {
    val headers = "NATS/1.0\r\nFoo: bar\r\n\r\n"
    val payload = "test"
    val headerLen = headers.length
    val totalLen = headerLen + payload.length
    val data = s"HMSG FOO 1 INBOX.X $headerLen $totalLen\r\n${headers}${payload}\r\n"

    parseString(data).map { frames =>
      frames.head match
        case NatsFrame.HMsgFrame(_, _, replyTo, hdrs, _, _) =>
          assertEquals(replyTo, Some("INBOX.X"))
          assertEquals(hdrs.get("Foo"), Some("bar"))
        case other =>
          fail(s"Expected HMsgFrame, got $other")
    }
  }

  test("parse HMSG with status code (no responders)") {
    val headers = "NATS/1.0 503\r\n\r\n"
    val headerLen = headers.length
    val totalLen = headerLen
    val data = s"HMSG FOO 1 $headerLen $totalLen\r\n${headers}\r\n"

    parseString(data).map { frames =>
      frames.head match
        case NatsFrame.HMsgFrame(_, _, _, _, statusCode, payload) =>
          assertEquals(statusCode, Some(503))
          assertEquals(payload.size, 0)
        case other =>
          fail(s"Expected HMsgFrame, got $other")
    }
  }

  test("parse HMSG with multiple headers") {
    val headers = "NATS/1.0\r\nX-One: 1\r\nX-Two: 2\r\nX-Three: 3\r\n\r\n"
    val payload = "data"
    val headerLen = headers.length
    val totalLen = headerLen + payload.length
    val data = s"HMSG FOO 1 $headerLen $totalLen\r\n${headers}${payload}\r\n"

    parseString(data).map { frames =>
      frames.head match
        case NatsFrame.HMsgFrame(_, _, _, hdrs, _, _) =>
          assertEquals(hdrs.get("X-One"), Some("1"))
          assertEquals(hdrs.get("X-Two"), Some("2"))
          assertEquals(hdrs.get("X-Three"), Some("3"))
        case other =>
          fail(s"Expected HMsgFrame, got $other")
    }
  }

  // --- Chunked Input Tests ---

  test("parse control line split across chunks") {
    val chunks = List(
      toBytes("PI"),
      toBytes("NG"),
      toBytes("\r\n")
    )
    parseChunks(chunks).map { frames =>
      assertEquals(frames, List(NatsFrame.PingFrame))
    }
  }

  test("parse control line with CRLF split across chunks") {
    val chunks = List(
      toBytes("PING\r"),
      toBytes("\n")
    )
    parseChunks(chunks).map { frames =>
      assertEquals(frames, List(NatsFrame.PingFrame))
    }
  }

  test("parse MSG with payload split across chunks") {
    val chunks = List(
      toBytes("MSG FOO 1 10\r\n"),
      toBytes("Hello"),
      toBytes("World"),
      toBytes("\r\n")
    )
    parseChunks(chunks).map { frames =>
      assertEquals(frames.length, 1)
      frames.head match
        case NatsFrame.MsgFrame(_, _, _, payload) =>
          assertEquals(new String(payload.toArray, StandardCharsets.UTF_8), "HelloWorld")
        case other =>
          fail(s"Expected MsgFrame, got $other")
    }
  }

  test("parse MSG with control line and payload across many small chunks") {
    val data = "MSG FOO 1 5\r\nHello\r\n"
    val chunks = data.map(c => toBytes(c.toString)).toList
    parseChunks(chunks).map { frames =>
      assertEquals(frames.length, 1)
      frames.head match
        case NatsFrame.MsgFrame(subject, _, _, payload) =>
          assertEquals(subject, "FOO")
          assertEquals(new String(payload.toArray, StandardCharsets.UTF_8), "Hello")
        case other =>
          fail(s"Expected MsgFrame, got $other")
    }
  }

  test("parse HMSG with headers split across chunks") {
    val headers = "NATS/1.0\r\nX-Test: value\r\n\r\n"
    val payload = "data"
    val headerLen = headers.length
    val totalLen = headerLen + payload.length

    val chunks = List(
      toBytes(s"HMSG FOO 1 $headerLen $totalLen\r\n"),
      toBytes("NATS/1.0\r\n"),
      toBytes("X-Test: value\r\n"),
      toBytes("\r\n"),
      toBytes("data\r\n")
    )

    parseChunks(chunks).map { frames =>
      assertEquals(frames.length, 1)
      frames.head match
        case NatsFrame.HMsgFrame(_, _, _, hdrs, _, pay) =>
          assertEquals(hdrs.get("X-Test"), Some("value"))
          assertEquals(new String(pay.toArray, StandardCharsets.UTF_8), "data")
        case other =>
          fail(s"Expected HMsgFrame, got $other")
    }
  }

  // --- Multiple Frames in One Chunk ---

  test("parse multiple frames in single chunk") {
    val data = "PING\r\nPONG\r\n+OK\r\n"
    parseString(data).map { frames =>
      assertEquals(frames, List(NatsFrame.PingFrame, NatsFrame.PongFrame, NatsFrame.OkFrame))
    }
  }

  test("parse MSG frames concatenated in single chunk") {
    val data = "MSG A 1 3\r\nabc\r\nMSG B 2 3\r\nxyz\r\n"
    parseString(data).map { frames =>
      assertEquals(frames.length, 2)
      frames(0) match
        case NatsFrame.MsgFrame(subject, _, _, payload) =>
          assertEquals(subject, "A")
          assertEquals(new String(payload.toArray, StandardCharsets.UTF_8), "abc")
        case _ => fail("Expected MsgFrame")
      frames(1) match
        case NatsFrame.MsgFrame(subject, _, _, payload) =>
          assertEquals(subject, "B")
          assertEquals(new String(payload.toArray, StandardCharsets.UTF_8), "xyz")
        case _ => fail("Expected MsgFrame")
    }
  }

  // --- Large Payloads ---

  test("parse large payload") {
    val payloadSize = 100000
    val payload = "x" * payloadSize
    val data = s"MSG FOO 1 $payloadSize\r\n$payload\r\n"

    parseString(data).map { frames =>
      assertEquals(frames.length, 1)
      frames.head match
        case NatsFrame.MsgFrame(_, _, _, pay) =>
          assertEquals(pay.size, payloadSize)
        case other =>
          fail(s"Expected MsgFrame, got $other")
    }
  }

  test("parse large payload in small chunks") {
    val payloadSize = 10000
    val payload = "x" * payloadSize
    val data = s"MSG FOO 1 $payloadSize\r\n$payload\r\n"

    val chunkSize = 100
    val chunks = data.grouped(chunkSize).map(s => toBytes(s)).toList

    parseChunks(chunks).map { frames =>
      assertEquals(frames.length, 1)
      frames.head match
        case NatsFrame.MsgFrame(_, _, _, pay) =>
          assertEquals(pay.size, payloadSize)
        case other =>
          fail(s"Expected MsgFrame, got $other")
    }
  }

  // --- Binary Payload ---

  test("parse MSG with binary payload") {
    val binaryPayload = Array[Byte](0, 1, 2, 127, -128, -1)
    val header = "MSG FOO 1 6\r\n".getBytes(StandardCharsets.UTF_8)
    val crlf = "\r\n".getBytes(StandardCharsets.UTF_8)
    val fullData = header ++ binaryPayload ++ crlf

    parseBytes(fullData).map { frames =>
      assertEquals(frames.length, 1)
      frames.head match
        case NatsFrame.MsgFrame(_, _, _, payload) =>
          assertEquals(payload.toArray.toList, binaryPayload.toList)
        case other =>
          fail(s"Expected MsgFrame, got $other")
    }
  }

  // --- Error Cases ---

  test("fail on control line exceeding max length") {
    val config = ParserConfig(maxControlLineLength = 10, strictMode = true)
    val longLine = "A" * 20 + "\r\n"

    Stream.emit(toBytes(longLine))
      .unchunks
      .through(ProtocolParser.parseStream[IO](config))
      .compile
      .toList
      .attempt
      .map {
        case Left(err) =>
          assert(err.getMessage.contains("exceeds maximum length"))
        case Right(_) =>
          fail("Expected parse error")
      }
  }

  test("emit parse error for unrecognized command in non-strict mode") {
    val config = ParserConfig(strictMode = false)

    Stream.emit(toBytes("UNKNOWN command\r\n"))
      .unchunks
      .through(ProtocolParser.parseStream[IO](config))
      .compile
      .toList
      .map { frames =>
        assertEquals(frames.length, 1)
        frames.head match
          case NatsFrame.ParseErrorFrame(msg, _) =>
            assert(msg.contains("Unrecognized command"))
          case other =>
            fail(s"Expected ParseErrorFrame, got $other")
      }
  }

  test("fail on invalid MSG format") {
    val config = ParserConfig(strictMode = true)

    Stream.emit(toBytes("MSG only_subject\r\n"))
      .unchunks
      .through(ProtocolParser.parseStream[IO](config))
      .compile
      .toList
      .attempt
      .map {
        case Left(err) =>
          assert(err.getMessage.contains("Invalid MSG"))
        case Right(_) =>
          fail("Expected parse error")
      }
  }

  test("fail on payload size exceeding limit") {
    val config = ParserConfig(maxPayloadLimit = Some(100), strictMode = true)
    val data = "MSG FOO 1 1000\r\n" + ("x" * 1000) + "\r\n"

    Stream.emit(toBytes(data))
      .unchunks
      .through(ProtocolParser.parseStream[IO](config))
      .compile
      .toList
      .attempt
      .map {
        case Left(err) =>
          assert(err.getMessage.contains("exceeds maximum"))
        case Right(_) =>
          fail("Expected parse error")
      }
  }

  // --- Edge Cases ---

  test("parse empty stream") {
    parseChunks(List.empty).map { frames =>
      assertEquals(frames, List.empty)
    }
  }

  test("handle payload with embedded CRLF patterns") {
    val payload = "line1\r\nline2\r\nline3"
    val data = s"MSG FOO 1 ${payload.length}\r\n$payload\r\n"

    parseString(data).map { frames =>
      assertEquals(frames.length, 1)
      frames.head match
        case NatsFrame.MsgFrame(_, _, _, pay) =>
          assertEquals(new String(pay.toArray, StandardCharsets.UTF_8), payload)
        case other =>
          fail(s"Expected MsgFrame, got $other")
    }
  }

  test("parse sequence of different frame types") {
    val infoJson = """{"server_id":"x","version":"2.0","proto":1,"go":"1","host":"h","port":4222,"max_payload":1024}"""
    val data = s"INFO $infoJson\r\nPING\r\nMSG FOO 1 4\r\ntest\r\nPONG\r\n+OK\r\n"

    parseString(data).map { frames =>
      assertEquals(frames.length, 5)
      assert(frames(0).isInstanceOf[NatsFrame.InfoFrame])
      assertEquals(frames(1), NatsFrame.PingFrame)
      assert(frames(2).isInstanceOf[NatsFrame.MsgFrame])
      assertEquals(frames(3), NatsFrame.PongFrame)
      assertEquals(frames(4), NatsFrame.OkFrame)
    }
  }
