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
import com.comcast.ip4s.{Host, Port}
import com.github.plokhotnyuk.jsoniter_scala.core.JsonValueCodec
import com.github.plokhotnyuk.jsoniter_scala.macros.JsonCodecMaker
import fs2.nats.client.{ClientConfig, NatsClient}
import fs2.nats.micro.*
import fs2.nats.protocol.Headers
import scala.concurrent.duration.*

/** Advanced micro services example (ADR-32):
  *
  *   - subject captures bound to domain classes instead of raw strings: one
  *     `*` token decodes to `TenantId`, a two-capture pattern to
  *     `(TenantId, OrderId)` — each via a `TokenCodec` built with `imap`
  *   - a non-empty JSON request body (`AddOrder`) and JSON responses via
  *     `Payload.json` with jsoniter-derived codecs
  *   - a typed error ADT (`OrderError`) instead of raw `(code, message)`
  *     pairs: both sides pattern match on the same cases, the wire carries
  *     only the ADR-32 error headers
  *   - payload schemas via `Payload.withSchema`, published in the `$SRV.INFO`
  *     endpoint metadata as `request_schema`/`response_schema` (the ADR-32
  *     successor of the retired `$SRV.SCHEMA` verb)
  *   - service- and endpoint-level metadata (`ServiceConfig.withMetadata`,
  *     `Rpc.withMetadata`), also visible through discovery
  *   - headers in both directions: the client attaches request headers with
  *     `callWithHeaders`, the handler reads them with `handleWithHeaders` and
  *     answers with a `Reply` that carries the response headers
  *
  * Prerequisites:
  *   - Start a NATS server: docker run -p 4222:4222 nats:latest
  *   - `Payload.json` needs derived codecs; add to your build:
  *     "com.github.plokhotnyuk.jsoniter-scala" %% "jsoniter-scala-macros" %
  *     "2.40.1" % "compile-internal"
  *
  * Run with: sbt "runMain fs2.nats.examples.MicroAdvancedExample"
  */
object MicroAdvancedExample extends IOApp:

  // ---- domain ------------------------------------------------------------

  final case class TenantId(value: String)
  final case class OrderId(value: Long)

  final case class AddOrder(item: String, quantity: Int)
  final case class Order(id: Long, item: String, quantity: Int)

  /** Typed service errors. `decode` must be total, so there is a catch-all
    * `Unknown` case for codes this version does not know.
    */
  enum OrderError:
    case NotFound(description: String)
    case InvalidQuantity(description: String)
    case Unknown(code: Int, description: String)

  // ---- shared API module -------------------------------------------------

  /** In a real project this object lives in a module the server and every
    * client depend on; handlers and calls stay in sync by construction.
    */
  object ShopApi:

    /** One `*` token per domain class: wrap/unwrap around the built-ins. */
    given TokenCodec[TenantId] = TokenCodec.string.imap(TenantId.apply)(_.value)
    given TokenCodec[OrderId] = TokenCodec.long.imap(OrderId.apply)(_.value)

    given JsonValueCodec[AddOrder] = JsonCodecMaker.make
    given JsonValueCodec[Order] = JsonCodecMaker.make

    val orderErr: ServiceErr[OrderError] = ServiceErr.from(
      encode = {
        case OrderError.NotFound(d)        => (404, d)
        case OrderError.InvalidQuantity(d) => (422, d)
        case OrderError.Unknown(code, d)   => (code, d)
      },
      decode = (code, description) =>
        code match
          case 404   => OrderError.NotFound(description)
          case 422   => OrderError.InvalidQuantity(description)
          case other => OrderError.Unknown(other, description)
    )

    /** shop.<tenant>.orders.get.<id> — two captures bound to a tuple of
      * domain classes; arity is checked at compile time. The endpoint
      * metadata shows up in this endpoint's `$SRV.INFO` entry.
      */
    val get = Rpc(
      name = "get",
      subject = pattern["shop.*.orders.get.*"].bind[(TenantId, OrderId)],
      in = Payload.empty,
      err = orderErr,
      out = Payload.json[Order]
    ).withMetadata(Map("stability" -> "stable", "owner" -> "orders-team"))

    private val addOrderSchema =
      """{"type":"object","properties":{"item":{"type":"string"},"quantity":{"type":"integer"}},"required":["item","quantity"]}"""
    private val orderSchema =
      """{"type":"object","properties":{"id":{"type":"integer"},"item":{"type":"string"},"quantity":{"type":"integer"}}}"""

    /** shop.<tenant>.orders.add — one capture plus a JSON request body. The
      * attached schemas show up in `$SRV.INFO` as `request_schema` /
      * `response_schema` in this endpoint's metadata.
      */
    val add = Rpc(
      name = "add",
      subject = pattern["shop.*.orders.add"].bind[TenantId],
      in = Payload.withSchema(Payload.json[AddOrder], addOrderSchema),
      err = orderErr,
      out = Payload.withSchema(Payload.json[Order], orderSchema)
    )

  // ---- server + client ---------------------------------------------------

  override def run(args: List[String]): IO[ExitCode] =
    val config = ClientConfig(
      host = Host.fromString("localhost").get,
      port = Port.fromInt(4222).get
    )

    NatsClient
      .connect[IO](config)
      .use { client =>
        for
          store <- Ref.of[IO, (Long, Map[(TenantId, OrderId), Order])](
            (1L, Map.empty)
          )

          handlers = List(
            // Params arrive already decoded: (TenantId, OrderId). The header
            // variant additionally receives the request headers and returns a
            // Reply, so the response can carry headers of its own — here the
            // trace id is echoed back to the caller.
            ShopApi.get.handleWithHeaders[IO] { case ((tenant, id), headers, _) =>
              val trace = headers.get("X-Trace-Id").getOrElse("<none>")
              IO.println(s"  [server] get ${id.value} for '${tenant.value}', trace=$trace") *>
                store.get.map { case (_, orders) =>
                  orders
                    .get((tenant, id))
                    .toRight(
                      OrderError.NotFound(
                        s"order ${id.value} not found for tenant '${tenant.value}'"
                      )
                    )
                    .map(order => Reply(order, Headers("X-Trace-Id" -> trace)))
                }
            },
            // Params: TenantId; body: AddOrder decoded from JSON.
            ShopApi.add.handle[IO] { (tenant, req) =>
              if req.quantity <= 0 then
                IO.pure(
                  Left(
                    OrderError.InvalidQuantity(
                      s"quantity must be positive, got ${req.quantity}"
                    )
                  )
                )
              else
                store.modify { case (next, orders) =>
                  val order = Order(next, req.item, req.quantity)
                  (
                    (next + 1, orders.updated((tenant, OrderId(next)), order)),
                    Right(order)
                  )
                }
            }
          )

          serviceConfig = ServiceConfig("shop", "1.0.0")
            .withDescription("multi-tenant order service")
            // Service-level metadata: top level of PING/INFO/STATS responses.
            .withMetadata(Map("region" -> "eu-central"))

          _ <- NatsService(client, serviceConfig, handlers).use { _ =>
            IO.sleep(100.millis) *> typedCalls(client) *> schemaInfo(client)
          }

          _ <- IO.println("\nExample completed successfully!")
        yield ExitCode.Success
      }
      .handleErrorWith { err =>
        IO.println(s"Error: ${err.getMessage}").as(ExitCode.Error)
      }

  private def typedCalls(client: NatsClient[IO]): IO[Unit] =
    val micro = Micro(client)
    val acme = TenantId("acme")
    IO.println("\n--- Typed calls ---") *> {
      for
        // Non-empty request body: (params, body) on the 3-arg call.
        added <- micro.call(ShopApi.add)(acme, AddOrder("flat white", 2))
        _ <- IO.println(s"add: $added")

        // I = Unit sugar: only params, here the (TenantId, OrderId) tuple.
        found <- micro.call(ShopApi.get)((acme, OrderId(1)))
        _ <- IO.println(s"get 1: $found")

        // Same endpoint with a request header; the handler reads it via
        // handleWithHeaders and echoes it back on the Reply, so this call sees
        // both the value and the response headers.
        traced <- micro.callWithHeaders(ShopApi.get)(
          (acme, OrderId(1)),
          (),
          Headers("X-Trace-Id" -> "trace-42")
        )
        _ <- traced match
          case Right(reply) =>
            IO.println(
              s"get 1 with trace header: ${reply.value}, " +
                s"reply headers: ${reply.headers.entries.toList}"
            )
          case Left(err) => IO.println(s"get 1 with trace header: $err")

        // Both error cases come back as the shared ADT, nothing is raised.
        rejected <- micro.call(ShopApi.add)(acme, AddOrder("espresso", 0))
        _ <- IO.println(s"add quantity 0: $rejected")

        missing <- micro.call(ShopApi.get)((TenantId("globex"), OrderId(1)))
        _ <- missing match
          case Left(OrderError.NotFound(d)) => IO.println(s"get as globex: NotFound($d)")
          case other                        => IO.println(s"get as globex: $other")
      yield ()
    }

  /** The schemas attached with `Payload.withSchema` and the metadata from
    * `withMetadata` are visible to any client via discovery, e.g.
    * `nats micro info shop`.
    */
  private def schemaInfo(client: NatsClient[IO]): IO[Unit] =
    IO.println("\n--- $SRV.INFO with metadata and schemas ---") *>
      client.request("$SRV.INFO.shop", fs2.Chunk.empty).flatMap { reply =>
        IO.println(reply.payloadAsString)
      }
