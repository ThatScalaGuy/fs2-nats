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

package fs2.nats.examples

import cats.effect.{ExitCode, IO, IOApp, Ref}
import cats.syntax.all.*
import com.comcast.ip4s.{Host, Port}
import fs2.Chunk
import fs2.nats.client.{ClientConfig, NatsClient}
import fs2.nats.micro.*
import scala.concurrent.duration.*

/** Micro services example (ADR-32): endpoints are defined once as shared `Rpc`
  * values, served with `NatsService` and called with `Micro`. Service errors
  * come back as `Left`, discovery answers on `$SRV.*`.
  *
  * Prerequisites:
  *   - Start a NATS server: docker run -p 4222:4222 nats:latest
  *
  * Run with: sbt "runMain fs2.nats.examples.MicroExample"
  */
object MicroExample extends IOApp:

  /** Shared endpoint definitions: the server attaches handlers to these, the
    * client calls them. In a real project this object lives in a module both
    * sides depend on.
    */
  object OrdersApi:

    /** orders.get.<id> — no request body, replies with the order text. */
    val get = Rpc(
      name = "get",
      subject = pattern["orders.get.*"].bind[String],
      in = Payload.empty,
      err = ServiceErr.plain,
      out = Payload.string
    )

    /** orders.add — request body is the order text, replies with the new id. */
    val add = Rpc(
      name = "add",
      subject = pattern["orders.add"],
      in = Payload.string,
      err = ServiceErr.plain,
      out = Payload.string
    )

  override def run(args: List[String]): IO[ExitCode] =
    val config = ClientConfig(
      host = Host.fromString("localhost").get,
      port = Port.fromInt(4222).get
    )

    NatsClient
      .connect[IO](config)
      .use { client =>
        for
          store <- Ref.of[IO, (Int, Map[String, String])]((1, Map.empty))

          handlers = List(
            OrdersApi.get.handle[IO] { (id, _) =>
              store.get.map { case (_, orders) =>
                orders.get(id).toRight((404, s"order '$id' not found"))
              }
            },
            OrdersApi.add.handle[IO] { (_, order) =>
              store.modify { case (next, orders) =>
                (
                  (next + 1, orders.updated(next.toString, order)),
                  Right(next.toString)
                )
              }
            }
          )

          serviceConfig = ServiceConfig("orders", "1.0.0")
            .withDescription("in-memory order service")

          _ <- NatsService(client, serviceConfig, handlers).use { svc =>
            IO.sleep(100.millis) *>
              typedCalls(client) *>
              discovery(client) *>
              statsExample(svc)
          }

          _ <- IO.println("\nExample completed successfully!")
        yield ExitCode.Success
      }
      .handleErrorWith { err =>
        IO.println(s"Error: ${err.getMessage}").as(ExitCode.Error)
      }

  private def typedCalls(client: NatsClient[IO]): IO[Unit] =
    val micro = Micro(client)
    IO.println("\n--- Typed calls ---") *> {
      for
        added <- micro.call(OrdersApi.add)((), "2x flat white")
        _ <- IO.println(s"add: $added")

        id = added.getOrElse("1")
        found <- micro.call(OrdersApi.get)(id)
        _ <- IO.println(s"get $id: $found")

        // Unknown id: the handler's Left comes back typed, nothing is raised.
        missing <- micro.call(OrdersApi.get)("999")
        _ <- IO.println(s"get 999: $missing")
      yield ()
    }

  private def discovery(client: NatsClient[IO]): IO[Unit] =
    IO.println("\n--- Discovery ---") *>
      client.request("$SRV.PING", Chunk.empty).flatMap { reply =>
        IO.println(s"PING:  ${reply.payloadAsString}")
      } *>
      client.request("$SRV.INFO.orders", Chunk.empty).flatMap { reply =>
        IO.println(s"INFO:  ${reply.payloadAsString}")
      } *>
      client.request("$SRV.STATS.orders", Chunk.empty).flatMap { reply =>
        IO.println(s"STATS: ${reply.payloadAsString}")
      }

  private def statsExample(svc: NatsService[IO]): IO[Unit] =
    IO.println("\n--- Local stats ---") *>
      svc.stats.flatMap { s =>
        s.endpoints.traverse_ { e =>
          IO.println(
            s"  ${e.name}: ${e.numRequests} requests, ${e.numErrors} errors, " +
              s"avg ${e.averageProcessingTime.toMicros}us"
          )
        }
      } *>
      svc.reset *>
      IO.println("  counters reset")
