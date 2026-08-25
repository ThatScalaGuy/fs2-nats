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

package fs2.nats.micro

import cats.effect.{IO, Ref, Resource}
import fs2.nats.client.{ClientEvent, NatsClient}
import fs2.nats.jetstream.{JetStream, JetStreamConfig}
import fs2.nats.protocol.{Headers, Info}
import fs2.nats.subscriptions.NatsMessage
import fs2.{Chunk, Stream}

import java.nio.charset.StandardCharsets
import java.time.Instant
import scala.concurrent.duration.FiniteDuration

/** What the endpoint loop actually publishes for one request: the reply body
  * and the headers taken from the handler's [[Reply]].
  */
class ServiceReplySpec extends munit.CatsEffectSuite:

  private final case class Published(
      subject: String,
      payload: Chunk[Byte],
      headers: Headers
  )

  private def die[A](what: String): IO[A] =
    IO.raiseError(new AssertionError(s"stub $what must not be called"))

  /** Records `publish`; everything the request path does not touch raises. */
  private final class RecordingClient(sink: Ref[IO, Vector[Published]])
      extends NatsClient[IO]:
    def publish(
        subject: String,
        payload: Chunk[Byte],
        headers: Headers,
        replyTo: Option[String]
    ): IO[Unit] = sink.update(_ :+ Published(subject, payload, headers))
    def subscribe(
        subject: String,
        queueGroup: Option[String]
    ): Resource[IO, Stream[IO, NatsMessage]] =
      Resource.eval(die("subscribe"))
    def request(
        subject: String,
        payload: Chunk[Byte],
        headers: Headers,
        timeout: FiniteDuration
    ): IO[NatsMessage] = die("request")
    private[nats] def requestAsync(
        subject: String,
        payload: Chunk[Byte],
        headers: Headers,
        timeout: FiniteDuration,
        onSettle: IO[Unit]
    ): IO[IO[NatsMessage]] = die("requestAsync")
    def jetStream(config: JetStreamConfig): Resource[IO, JetStream[IO]] =
      Resource.eval(die("jetStream"))
    def events: Stream[IO, ClientEvent] = Stream.exec(die("events"))
    def close: IO[Unit] = die("close")
    def serverInfo: IO[Info] = die("serverInfo")
    def isConnected: IO[Boolean] = die("isConnected")

  private val echo: Rpc[Unit, String, (Int, String), String] =
    Rpc(
      "echo",
      pattern["svc.echo"],
      Payload.string,
      ServiceErr.plain,
      Payload.string
    )

  private val request: NatsMessage =
    NatsMessage(
      subject = "svc.echo",
      replyTo = Some("_INBOX.reply"),
      headers = Headers.empty,
      payload = Chunk.array("hi".getBytes(StandardCharsets.UTF_8)),
      sid = 1L
    )

  /** Drives one request through the real endpoint loop. */
  private def replyTo(handler: MicroHandler[IO]): IO[Published] =
    val impl = handler.asInstanceOf[MicroHandler.Impl[IO, Any, Any, Any, Any]]
    for
      sink <- Ref.of[IO, Vector[Published]](Vector.empty)
      state <- Ref.of[IO, ServiceImpl.State](
        ServiceImpl.State(Instant.EPOCH, ServiceImpl.zeroCounters(List(impl)))
      )
      runtime = new ServiceRuntime[IO](
        new RecordingClient(sink),
        ServiceConfig("svc", "1.0.0"),
        "id",
        List(impl),
        state
      )
      _ <- runtime.endpointLoop(impl, Stream.emit(request)).compile.drain
      published <- sink.get
    yield
      assertEquals(published.size, 1, "expected exactly one reply")
      published.head

  private def asString(c: Chunk[Byte]): String =
    new String(c.toArray, StandardCharsets.UTF_8)

  test("handle replies without headers") {
    replyTo(echo.handle[IO]((_, in) => IO.pure(Right(s"echo:$in")))).map { p =>
      assertEquals(p.subject, "_INBOX.reply")
      assertEquals(asString(p.payload), "echo:hi")
      assertEquals(p.headers, Headers.empty)
    }
  }

  test("handleWithHeaders puts the Reply headers on the success reply") {
    val handler = echo.handleWithHeaders[IO] { (_, _, in) =>
      IO.pure(Right(Reply(s"echo:$in", Headers("X-Cache" -> "hit"))))
    }
    replyTo(handler).map { p =>
      assertEquals(asString(p.payload), "echo:hi")
      assertEquals(p.headers, Headers("X-Cache" -> "hit"))
    }
  }

  test("a Reply header cannot inject a line into the reply header block") {
    val handler = echo.handleWithHeaders[IO] { (_, _, _) =>
      IO.pure(
        Right(
          Reply(
            "ok",
            Headers(
              "X-Echo\r\nX-Injected" -> "a\r\nNats-Service-Error-Code: 500"
            )
          )
        )
      )
    }
    replyTo(handler).map { p =>
      assertEquals(
        p.headers,
        Headers("X-Echo  X-Injected" -> "a  Nats-Service-Error-Code: 500")
      )
      assert(!asString(p.headers.toBytes).contains("\r\nNats-Service-Error"))
    }
  }

  test("an error reply carries the ADR-32 headers and an empty body") {
    val handler = echo.handleWithHeaders[IO] { (_, _, _) =>
      IO.pure(Left((404, "nope")))
    }
    replyTo(handler).map { p =>
      assertEquals(p.payload, Chunk.empty[Byte])
      assertEquals(p.headers.get("Nats-Service-Error-Code"), Some("404"))
      assertEquals(p.headers.get("Nats-Service-Error"), Some("nope"))
    }
  }
