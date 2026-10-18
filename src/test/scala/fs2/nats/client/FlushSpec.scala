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

package fs2.nats.client

import cats.effect.{Deferred, IO, Resource}
import cats.effect.std.Queue
import cats.syntax.all.*
import com.comcast.ip4s.*
import fs2.{Chunk, Stream, text}
import fs2.io.net.Network
import fs2.nats.errors.NatsError
import munit.CatsEffectSuite
import scala.concurrent.duration.*

/** `flush` against a fake server that sends INFO, then answers each PING only
  * when the test hands it a permit. No real NATS server needed.
  */
class FlushSpec extends CatsEffectSuite:

  override def munitIOTimeout: Duration = 20.seconds

  /** @param permits
    *   one element per PONG the server may send
    * @param pings
    *   gets one element per PING the server reads
    * @param kill
    *   completing it drops the connection
    */
  private final case class FakeServer(
      port: Port,
      permits: Queue[IO, Unit],
      pings: Queue[IO, Unit],
      kill: Deferred[IO, Unit]
  )

  private val Pong = Chunk.array("PONG\r\n".getBytes)

  private def info(port: Port): Chunk[Byte] =
    Chunk.array(
      ("INFO {\"server_id\":\"fake\",\"version\":\"2.10.0\",\"proto\":1," +
        "\"go\":\"go1.22\",\"host\":\"127.0.0.1\"," +
        s"\"port\":${port.value},\"max_payload\":1048576}\r\n").getBytes
    )

  /** Accepts a single connection, so a reconnect finds no server. */
  private def fakeServer: Resource[IO, FakeServer] =
    for
      server <- Network[IO].bind(SocketAddress(ip"127.0.0.1", port"0"))
      port = server.address match
        case sa: SocketAddress[?] => sa.port
        case other                => fail(s"unexpected address $other")
      permits <- Resource.eval(Queue.unbounded[IO, Unit])
      pings <- Resource.eval(Queue.unbounded[IO, Unit])
      kill <- Resource.eval(Deferred[IO, Unit])
      _ <- server.accept
        .take(1)
        .flatMap { socket =>
          // The reader never blocks on a permit, so `pings` sees every PING.
          val answer = Stream.eval(Queue.unbounded[IO, Unit]).flatMap { owed =>
            val read = socket.reads
              .through(text.utf8.decode)
              .through(text.lines)
              .filter(_ == "PING")
              .foreach(_ => pings.offer(()) *> owed.offer(()))
            val write = Stream.repeatEval(
              owed.take *> permits.take *> socket.write(Pong)
            )
            read.concurrently(write)
          }
          (Stream.eval(socket.write(info(port))) ++ answer)
            .interruptWhen(kill.get.attempt)
        }
        .compile
        .drain
        .background
    yield FakeServer(port, permits, pings, kill)

  private def client(port: Port): Resource[IO, NatsClient[IO]] =
    NatsClient.connect[IO](
      ClientConfig(
        host = host"127.0.0.1",
        port = port,
        backoff = BackoffConfig.fast.copy(maxRetries = Some(1))
      )
    )

  test("flush completes on its own PONG, not on the CONNECT PONG") {
    fakeServer.use { srv =>
      client(srv.port).use { c =>
        for
          _ <- srv.pings.take // PING sent with CONNECT
          fiber <- c.flush().start
          _ <- srv.pings.take // PING sent by flush
          _ <- srv.permits.offer(()) // answer only the CONNECT PING
          early <- fiber.join.timeout(300.millis).attempt
          _ = assert(early.isLeft, "flush completed on the CONNECT PONG")
          _ <- srv.permits.offer(())
          _ <- fiber.joinWithNever
        yield ()
      }
    }
  }

  test("many concurrent flushes all complete") {
    fakeServer.use { srv =>
      client(srv.port).use { c =>
        srv.permits.offer(()).replicateA_(51) *>
          (1 to 50).toList.parTraverse_(_ => c.flush())
      }
    }
  }

  test("flush times out when the server does not answer") {
    fakeServer.use { srv =>
      client(srv.port).use { c =>
        c.flush(200.millis).intercept[NatsError.Timeout].void
      }
    }
  }

  test("pending flush fails on disconnect, flush while offline fails fast") {
    fakeServer.use { srv =>
      client(srv.port).use { c =>
        for
          fiber <- c.flush(10.seconds).start
          _ <- srv.pings.take.replicateA_(2)
          _ <- srv.kill.complete(())
          lost <- fiber.joinWithNever.attempt
          offline <- c.flush().attempt
        yield
          assertEquals(
            lost,
            Left(NatsError.ConnectionFailed("connection lost"))
          )
          assertEquals(
            offline,
            Left(NatsError.ConnectionFailed("not connected"))
          )
      }
    }
  }

  test("a timed-out flush keeps later flushes matched to their own PONG") {
    fakeServer.use { srv =>
      client(srv.port).use { c =>
        for
          _ <- srv.pings.take // CONNECT
          _ <- c.flush(100.millis).intercept[NatsError.Timeout]
          _ <- srv.pings.take // timed-out flush, still on the wire
          fiber <- c.flush().start
          _ <- srv.pings.take
          _ <- srv.permits.offer(()).replicateA_(2) // CONNECT + timed-out one
          early <- fiber.join.timeout(300.millis).attempt
          _ = assert(early.isLeft, "flush completed on an older PONG")
          _ <- srv.permits.offer(())
          _ <- fiber.joinWithNever
        yield ()
      }
    }
  }

  test("pending flush fails with ClientClosed on close") {
    fakeServer.use { srv =>
      client(srv.port).use { c =>
        for
          fiber <- c.flush(10.seconds).start
          _ <- srv.pings.take.replicateA_(2)
          _ <- c.close
          res <- fiber.joinWithNever.attempt
        yield assertEquals(res, Left(NatsError.ClientClosed))
      }
    }
  }

  test("flush after close fails with ClientClosed") {
    fakeServer.use { srv =>
      client(srv.port).use { c =>
        c.close *> c.flush().intercept[NatsError.ClientClosed.type].void
      }
    }
  }
