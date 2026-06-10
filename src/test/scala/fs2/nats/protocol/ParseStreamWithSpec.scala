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
import fs2.{Chunk, Stream}
import munit.CatsEffectSuite
import fs2.nats.subscriptions.NatsMessage
import java.nio.charset.StandardCharsets

/** P1.3 correctness gate: when the parser is given the `NatsMessage` builder,
  * MSG/HMSG *data* frames must surface as a single already-built `NatsMessage`
  * (no intermediate `MsgFrame`), while INFO/PING/PONG/+OK/-ERR *control* frames
  * must still surface as their `NatsFrame` variants exactly as before.
  */
class ParseStreamWithSpec extends CatsEffectSuite:

  private def parse(s: String): IO[List[Frame]] =
    Stream
      .emit(Chunk.array(s.getBytes(StandardCharsets.UTF_8)))
      .unchunks
      .through(
        ProtocolParser.parseStreamWith[IO, Frame](NatsMessage.parserBuilder)
      )
      .compile
      .toList

  test("MSG surfaces as a NatsMessage (not a MsgFrame)") {
    parse("MSG events.x 7 5\r\nHello\r\n").map { frames =>
      assertEquals(frames.length, 1)
      frames.head match
        case m: NatsMessage =>
          assertEquals(m.subject, "events.x")
          assertEquals(m.sid, 7L)
          assertEquals(m.replyTo, None)
          assertEquals(m.payloadAsString, "Hello")
          assertEquals(m.headers, Headers.empty)
        case other => fail(s"Expected NatsMessage, got $other")
    }
  }

  test("MSG with reply-to surfaces as a NatsMessage") {
    parse("MSG events.x 7 _INBOX.1 2\r\nHi\r\n").map { frames =>
      frames.head match
        case m: NatsMessage =>
          assertEquals(m.replyTo, Some("_INBOX.1"))
          assertEquals(m.payloadAsString, "Hi")
        case other => fail(s"Expected NatsMessage, got $other")
    }
  }

  test("HMSG surfaces as a NatsMessage with headers and status") {
    val frame =
      "HMSG events.x 7 27 31\r\nNATS/1.0 100 Idle\r\nA: b\r\n\r\ndata\r\n"
    parse(frame).map { frames =>
      assertEquals(frames.length, 1)
      frames.head match
        case m: NatsMessage =>
          assertEquals(m.subject, "events.x")
          assertEquals(m.status, Some(100))
          assertEquals(m.statusDescription, Some("Idle"))
          assertEquals(m.headers.get("A"), Some("b"))
          assertEquals(m.payloadAsString, "data")
        case other => fail(s"Expected NatsMessage, got $other")
    }
  }

  test("PING/PONG/+OK still surface as NatsFrame control frames") {
    parse("PING\r\nPONG\r\n+OK\r\n").map { frames =>
      assertEquals(
        frames,
        List[Frame](
          NatsFrame.PingFrame,
          NatsFrame.PongFrame,
          NatsFrame.OkFrame
        )
      )
    }
  }

  test("-ERR still surfaces as a NatsFrame.ErrFrame") {
    parse("-ERR 'Unknown Protocol Operation'\r\n").map { frames =>
      assertEquals(
        frames,
        List[Frame](NatsFrame.ErrFrame("Unknown Protocol Operation"))
      )
    }
  }

  test("INFO still surfaces as a NatsFrame.InfoFrame") {
    val infoJson =
      """{"server_id":"test","version":"2.9.0","proto":1,"go":"go1.19","host":"0.0.0.0","port":4222,"max_payload":1048576}"""
    parse(s"INFO $infoJson\r\n").map { frames =>
      frames.head match
        case NatsFrame.InfoFrame(info) =>
          assertEquals(info.serverId, "test")
          assertEquals(info.maxPayload, 1048576L)
        case other => fail(s"Expected InfoFrame, got $other")
    }
  }

  test("a mixed stream routes control to NatsFrame and data to NatsMessage") {
    parse("PING\r\nMSG a.b 1 2\r\nhi\r\n+OK\r\n").map { frames =>
      assertEquals(frames.length, 3)
      assert(frames(0) == NatsFrame.PingFrame)
      frames(1) match
        case m: NatsMessage => assertEquals(m.payloadAsString, "hi")
        case other          => fail(s"Expected NatsMessage, got $other")
      assert(frames(2) == NatsFrame.OkFrame)
    }
  }
