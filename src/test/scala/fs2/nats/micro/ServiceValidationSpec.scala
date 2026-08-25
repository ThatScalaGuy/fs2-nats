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

import cats.effect.{IO, Resource}
import fs2.nats.client.{ClientEvent, NatsClient}
import fs2.nats.jetstream.{JetStream, JetStreamConfig}
import fs2.nats.protocol.{Headers, Info}
import fs2.nats.subscriptions.NatsMessage
import fs2.{Chunk, Stream}

import scala.concurrent.duration.FiniteDuration

class ServiceValidationSpec extends munit.CatsEffectSuite:

  /** Raised by the positive-control stub when `subscribe` is reached. */
  private final class SubscribeMarker
      extends RuntimeException("subscribe reached")

  private def die[A](what: String): IO[A] =
    IO.raiseError(new AssertionError(s"stub $what must not be called"))

  /** All members raise; `subscribe` acquisition runs `onSubscribe` so the
    * positive control can prove validation happens first.
    */
  private final class StubClient(onSubscribe: IO[Stream[IO, NatsMessage]])
      extends NatsClient[IO]:
    def publish(
        subject: String,
        payload: Chunk[Byte],
        headers: Headers,
        replyTo: Option[String]
    ): IO[Unit] = die("publish")
    def subscribe(
        subject: String,
        queueGroup: Option[String]
    ): Resource[IO, Stream[IO, NatsMessage]] = Resource.eval(onSubscribe)
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

  private def neverSubscribes: NatsClient[IO] = new StubClient(die("subscribe"))

  private def echo(name: String): MicroHandler[IO] =
    Rpc(
      name,
      pattern["svc.echo"],
      Payload.empty,
      ServiceErr.plain,
      Payload.empty
    )
      .handle[IO]((_, _) => IO.pure(Right(())))

  test("acquisition fails for an invalid service name") {
    interceptIO[IllegalArgumentException](
      NatsService(
        neverSubscribes,
        ServiceConfig("bad name!", "1.0.0"),
        List(echo("e"))
      ).use_
    )
  }

  test("acquisition fails for a non-SemVer version") {
    interceptIO[IllegalArgumentException](
      NatsService(
        neverSubscribes,
        ServiceConfig("svc", "not-semver"),
        List(echo("e"))
      ).use_
    )
  }

  test("acquisition fails for an empty handler list") {
    interceptIO[IllegalArgumentException](
      NatsService(neverSubscribes, ServiceConfig("svc", "1.0.0"), Nil).use_
    )
  }

  test("acquisition fails for duplicate endpoint names") {
    interceptIO[IllegalArgumentException](
      NatsService(
        neverSubscribes,
        ServiceConfig("svc", "1.0.0"),
        List(echo("dup"), echo("dup"))
      ).use_
    )
  }

  test("a valid config passes validation and only then reaches subscribe") {
    val client = new StubClient(IO.raiseError(new SubscribeMarker))
    interceptIO[SubscribeMarker](
      NatsService(client, ServiceConfig("svc", "1.0.0"), List(echo("e"))).use_
    )
  }
