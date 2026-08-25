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

import cats.effect.IO
import cats.syntax.all.*
import com.comcast.ip4s.{Host, Port}
import fs2.Chunk
import fs2.nats.client.{BackoffConfig, ClientConfig, NatsClient}
import fs2.nats.errors.NatsError
import munit.CatsEffectSuite
import scala.concurrent.duration.*

/** Integration tests for the NATS micro service layer (ADR-32).
  *
  * These tests require a running NATS server. Start one with: docker-compose up
  * -d
  *
  * Subject patterns are compile-time literals, so run-unique subjects are not
  * possible; each test uses its own literal prefix and a fresh service (with a
  * run-unique service name), and tests run sequentially.
  */
class MicroIntegrationSpec extends CatsEffectSuite:

  override def munitTimeout: Duration = 60.seconds

  private val natsHost = Host.fromString("localhost").get
  private val natsPort = Port.fromInt(4222).get

  private def clientConfig = ClientConfig(
    host = natsHost,
    port = natsPort,
    backoff = BackoffConfig.fast.copy(maxRetries = Some(3))
  )

  // "$SRV" would break inside a string interpolator, so discovery subjects
  // are built by plain concatenation.
  private val srv = "$SRV"

  test("typed round trip through a captured and a static endpoint") {
    val getRpc = Rpc(
      "get",
      pattern["it.micro.rt.orders.get.*"].bind[Int],
      Payload.empty,
      ServiceErr.plain,
      Payload.string
    )
    val echoRpc = Rpc(
      "echo",
      pattern["it.micro.rt.orders.echo"],
      Payload.string,
      ServiceErr.plain,
      Payload.string
    )

    NatsClient
      .connect[IO](clientConfig)
      .use { client =>
        val micro = Micro(client)
        val config =
          ServiceConfig(s"it-rt-${System.currentTimeMillis()}", "1.0.0")
        val handlers = List(
          getRpc.handle[IO]((id, _) => IO.pure(Right(s"order-$id"))),
          echoRpc.handle[IO]((_, in) => IO.pure(Right(s"echo:$in")))
        )

        NatsService[IO](client, config, handlers).use { _ =>
          for
            _ <- IO.sleep(150.millis)
            get <- micro.call(getRpc)(42)
            echo <- micro.call(echoRpc)((), "hello")
          yield
            assertEquals(get, Right("order-42"))
            assertEquals(echo, Right("echo:hello"))
        }
      }
      .timeout(15.seconds)
  }

  test("request headers round trip: callWithHeaders to handleWithHeaders") {
    val traceRpc = Rpc(
      "trace",
      pattern["it.micro.hdr.trace"],
      Payload.string,
      ServiceErr.plain,
      Payload.string
    )

    NatsClient
      .connect[IO](clientConfig)
      .use { client =>
        val micro = Micro(client)
        val config =
          ServiceConfig(s"it-hdr-${System.currentTimeMillis()}", "1.0.0")
        val handlers = List(
          traceRpc.handleWithHeaders[IO] { (_, headers, in) =>
            val trace = headers.get("X-Trace-Id").getOrElse("<none>")
            IO.pure(Right(s"$in/trace=$trace"))
          }
        )

        NatsService[IO](client, config, handlers).use { _ =>
          for
            _ <- IO.sleep(150.millis)
            withHeader <- micro.callWithHeaders(traceRpc)(
              (),
              "ping",
              fs2.nats.protocol.Headers("X-Trace-Id" -> "abc-123")
            )
            withoutHeader <- micro.call(traceRpc)((), "ping")
          yield
            assertEquals(withHeader, Right("ping/trace=abc-123"))
            assertEquals(withoutHeader, Right("ping/trace=<none>"))
        }
      }
      .timeout(15.seconds)
  }

  test("error paths come back as typed Lefts") {
    val leftRpc = Rpc(
      "left",
      pattern["it.micro.err.left"],
      Payload.empty,
      ServiceErr.plain,
      Payload.string
    )
    val boomRpc = Rpc(
      "boom",
      pattern["it.micro.err.boom"],
      Payload.empty,
      ServiceErr.plain,
      Payload.string
    )
    val syncBoomRpc = Rpc(
      "sync-boom",
      pattern["it.micro.err.syncboom"],
      Payload.empty,
      ServiceErr.plain,
      Payload.string
    )
    // Int written as UTF-8 text; decoding rejects anything else.
    val intText: Payload[Int] =
      Payload.string.imap(s => s.toIntOption.toRight(s"not an int: '$s'"))(
        _.toString
      )
    val parseRpc = Rpc(
      "parse",
      pattern["it.micro.err.parse"],
      intText,
      ServiceErr.plain,
      Payload.string
    )
    // Client-side view of the parse endpoint that can send arbitrary text.
    val looseParseRpc = Rpc(
      "parse",
      pattern["it.micro.err.parse"],
      Payload.string,
      ServiceErr.plain,
      Payload.string
    )

    NatsClient
      .connect[IO](clientConfig)
      .use { client =>
        val micro = Micro(client)
        val config =
          ServiceConfig(s"it-err-${System.currentTimeMillis()}", "1.0.0")
        val handlers = List(
          leftRpc.handle[IO]((_, _) => IO.pure(Left((404, "not found")))),
          boomRpc.handle[IO]((_, _) =>
            IO.raiseError(new RuntimeException("boom"))
          ),
          // Throws before returning an IO: must become a 500 reply, and the
          // endpoint must survive to answer the next request.
          syncBoomRpc.handle[IO]((_, _) => sys.error("sync-boom")),
          parseRpc.handle[IO]((_, n) => IO.pure(Right(s"n=$n")))
        )

        NatsService[IO](client, config, handlers).use { _ =>
          for
            _ <- IO.sleep(150.millis)
            left <- micro.call(leftRpc)(())
            boom <- micro.call(boomRpc)(())
            syncBoom <- micro.call(syncBoomRpc)(())
            syncBoomAgain <- micro.call(syncBoomRpc)(())
            bad <- micro.call(looseParseRpc)((), "not-a-number")
          yield
            assertEquals(left, Left((404, "not found")))
            assertEquals(boom, Left((500, "boom")))
            assertEquals(syncBoom, Left((500, "sync-boom")))
            assertEquals(syncBoomAgain, Left((500, "sync-boom")))
            bad match
              case Left((400, desc)) =>
                assert(clue(desc).contains("invalid request payload"))
              case other => fail(s"Expected Left((400, _)), got $other")
        }
      }
      .timeout(15.seconds)
  }

  test("discovery answers PING, targeted PING and INFO") {
    val getRpc = Rpc(
      "get",
      pattern["it.micro.disc.get.*"].bind[Int],
      Payload.withSchema(Payload.string, """{"type":"string"}"""),
      ServiceErr.plain,
      Payload.string
    )

    NatsClient
      .connect[IO](clientConfig)
      .use { client =>
        val name = s"it-disc-${System.currentTimeMillis()}"
        val config = ServiceConfig(name, "1.0.0")
        val handlers =
          List(getRpc.handle[IO]((id, in) => IO.pure(Right(s"$id:$in"))))

        NatsService[IO](client, config, handlers).use { service =>
          for
            _ <- IO.sleep(150.millis)
            ping <- client
              .request(srv + ".PING", Chunk.empty)
              .map(_.payloadAsString)
            targeted <- client
              .request(srv + ".PING." + name + "." + service.id, Chunk.empty)
              .map(_.payloadAsString)
            info <- client
              .request(srv + ".INFO." + name, Chunk.empty)
              .map(_.payloadAsString)
          yield
            assert(clue(ping).contains("ping_response"))
            assert(clue(ping).contains(name))
            assert(clue(targeted).contains("ping_response"))
            assert(clue(targeted).contains(service.id))
            assert(clue(info).contains("it.micro.disc.get.*"))
            assert(clue(info).contains("request_schema"))
        }
      }
      .timeout(15.seconds)
  }

  test("stats count requests and errors; reset zeroes them") {
    val workRpc = Rpc(
      "work",
      pattern["it.micro.stats.work.*"].bind[Int],
      Payload.empty,
      ServiceErr.plain,
      Payload.string
    )

    NatsClient
      .connect[IO](clientConfig)
      .use { client =>
        val micro = Micro(client)
        val name = s"it-stats-${System.currentTimeMillis()}"
        val config = ServiceConfig(name, "1.0.0")
        val handlers = List(workRpc.handle[IO] { (id, _) =>
          if id == 0 then IO.pure(Left((500, "fail")))
          else IO.pure(Right(s"ok-$id"))
        })

        NatsService[IO](client, config, handlers).use { service =>
          for
            _ <- IO.sleep(150.millis)
            results <- List(1, 2, 3, 4, 0).traverse(i => micro.call(workRpc)(i))
            // Counters are recorded after the reply is published; give the
            // server fiber a beat before reading them.
            _ <- IO.sleep(200.millis)
            statsJson <- client
              .request(srv + ".STATS." + name, Chunk.empty)
              .map(_.payloadAsString)
            stats <- service.stats
            _ <- service.reset
            afterReset <- service.stats
          yield
            assertEquals(results.count(_.isRight), 4)
            assertEquals(results.count(_.isLeft), 1)
            assert(clue(statsJson).contains("num_requests"))
            assert(clue(statsJson).contains("work"))
            val ep = stats.endpoints.find(_.name == "work").get
            assertEquals(ep.numRequests, 5L)
            assert(clue(ep.numErrors) >= 1L)
            val epReset = afterReset.endpoints.find(_.name == "work").get
            assertEquals(epReset.numRequests, 0L)
            assertEquals(epReset.numErrors, 0L)
        }
      }
      .timeout(15.seconds)
  }

  test("queue group answers each request exactly once across two instances") {
    val workRpc = Rpc(
      "work",
      pattern["it.micro.lb.work.*"].bind[Int],
      Payload.empty,
      ServiceErr.plain,
      Payload.string
    )

    NatsClient
      .connect[IO](clientConfig)
      .use { client =>
        val micro = Micro(client)
        val config =
          ServiceConfig(s"it-lb-${System.currentTimeMillis()}", "1.0.0")

        IO.ref(List.empty[String]).flatMap { seen =>
          def handlers(tag: String) = List(workRpc.handle[IO] { (id, _) =>
            seen.update(tag :: _).as(Right(s"$tag:$id"))
          })

          (
            NatsService[IO](client, config, handlers("a")),
            NatsService[IO](client, config, handlers("b"))
          ).tupled.use { _ =>
            for
              _ <- IO.sleep(150.millis)
              results <- (1 to 20).toList.parTraverse(i =>
                micro.call(workRpc)(i)
              )
              tags <- seen.get
            yield
              // Exactly-once per request; how the broker splits the load
              // between the two instances is not asserted.
              val answered =
                results.collect { case Right(s) => s.split(':').last.toInt }
              assertEquals(answered.length, 20)
              assertEquals(answered.toSet, (1 to 20).toSet)
              assertEquals(tags.length, 20)
          }
        }
      }
      .timeout(15.seconds)
  }

  test("release stops the control plane and the endpoints") {
    val hitRpc = Rpc(
      "hit",
      pattern["it.micro.lc.hit"],
      Payload.empty,
      ServiceErr.plain,
      Payload.string
    )

    NatsClient
      .connect[IO](clientConfig)
      .use { client =>
        val micro = Micro(client)
        val name = s"it-lc-${System.currentTimeMillis()}"
        val config = ServiceConfig(name, "1.0.0")
        val handlers = List(hitRpc.handle[IO]((_, _) => IO.pure(Right("alive"))))

        for
          allocated <- NatsService[IO](client, config, handlers).allocated
          (service, release) = allocated
          _ <- IO.sleep(150.millis)
          before <- micro.call(hitRpc)(())
          _ <- release
          _ <- IO.sleep(300.millis)
          ping <- client
            .request(
              srv + ".PING." + name + "." + service.id,
              Chunk.empty,
              timeout = 2.seconds
            )
            .attempt
          after <- micro.call(hitRpc)(()).attempt
        yield
          assertEquals(before, Right("alive"))
          ping match
            case Left(_: NatsError.Timeout)      => ()
            case Left(_: NatsError.NoResponders) => ()
            case other =>
              fail(s"Expected Timeout or NoResponders, got $other")
          after match
            case Left(_: NatsError.NoResponders) => ()
            case other => fail(s"Expected NoResponders, got $other")
      }
      .timeout(15.seconds)
  }
