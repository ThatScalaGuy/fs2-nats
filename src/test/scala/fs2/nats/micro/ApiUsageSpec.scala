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
import fs2.nats.protocol.Headers

/** The ADR-32 target usage from the design spec, as one shared API object.
  * Compile-level contract only: no server or client is started.
  */
object OrdersApi:

  final case class OrderId(value: String)

  given TokenCodec[OrderId] = TokenCodec.string.imap(OrderId(_))(_.value)

  sealed trait OrderError
  object OrderError:
    case object NotFound extends OrderError
    final case class Unknown(code: Int, description: String) extends OrderError

  val orderErr: ServiceErr[OrderError] =
    ServiceErr.from[OrderError](
      {
        case OrderError.NotFound      => (404, "order not found")
        case OrderError.Unknown(c, d) => (c, d)
      },
      (code, desc) =>
        if code == 404 then OrderError.NotFound
        else OrderError.Unknown(code, desc)
    )

  final case class Order(id: String, quantity: Int)

  val orderPayload: Payload[Order] =
    Payload.string.imap { s =>
      s.split(',') match
        case Array(id, qty) =>
          qty.toIntOption
            .toRight(s"not an Int: '$qty'")
            .map(Order(id, _))
        case _ => Left(s"not an Order: '$s'")
    }(o => s"${o.id},${o.quantity}")

  val get: Rpc[OrderId, Unit, OrderError, Order] =
    Rpc(
      "get",
      pattern["orders.get.*"].bind[OrderId],
      Payload.empty,
      orderErr,
      orderPayload
    )

  val add: Rpc[Unit, Order, OrderError, Unit] =
    Rpc("add", pattern["orders.add"], orderPayload, orderErr, Payload.empty)

class ApiUsageSpec extends munit.FunSuite:

  test("rpc definitions carry name, subject and defaults") {
    assertEquals(OrdersApi.get.name, "get")
    assertEquals(OrdersApi.get.subject.render, "orders.get.*")
    assertEquals(OrdersApi.get.queueGroup, None)
    assertEquals(OrdersApi.get.metadata, Map.empty[String, String])
    assertEquals(OrdersApi.add.name, "add")
    assertEquals(OrdersApi.add.subject.render, "orders.add")
    assertEquals(OrdersApi.add.queueGroup, None)
    assertEquals(OrdersApi.add.metadata, Map.empty[String, String])
  }

  test("the bound OrderId codec drives the subject") {
    assertEquals(
      OrdersApi.get.subject.fill(OrdersApi.OrderId("o1")),
      Right("orders.get.o1")
    )
    assertEquals(
      OrdersApi.get.subject.extract("orders.get.o1"),
      Right(OrdersApi.OrderId("o1"))
    )
  }

  test("withQueueGroup returns a copy") {
    val q = OrdersApi.get.withQueueGroup("orders-q")
    assertEquals(q.queueGroup, Some("orders-q"))
    assertEquals(q.name, OrdersApi.get.name)
    assertEquals(q.subject.render, OrdersApi.get.subject.render)
    assertEquals(q.metadata, OrdersApi.get.metadata)
    assertEquals(OrdersApi.get.queueGroup, None)
  }

  test("withMetadata returns a copy") {
    val m = OrdersApi.get.withMetadata(Map("owner" -> "sven"))
    assertEquals(m.metadata, Map("owner" -> "sven"))
    assertEquals(m.queueGroup, None)
    assertEquals(m.name, OrdersApi.get.name)
    assertEquals(OrdersApi.get.metadata, Map.empty[String, String])
  }

  test("handle and handleWithHeaders return MicroHandler[IO]") {
    val get: MicroHandler[IO] =
      OrdersApi.get.handle[IO] { (id, _) =>
        IO.pure(Right(OrdersApi.Order(id.value, 1)))
      }
    val getWithHeaders: MicroHandler[IO] =
      OrdersApi.get.handleWithHeaders[IO] { (id, _, _) =>
        IO.pure(
          Right(
            Reply(OrdersApi.Order(id.value, 1), Headers("X-Cache" -> "hit"))
          )
        )
      }
    val getFailing: MicroHandler[IO] =
      OrdersApi.get.handleWithHeaders[IO] { (_, _, _) =>
        IO.pure(Left(OrdersApi.OrderError.NotFound))
      }
    val add: MicroHandler[IO] =
      OrdersApi.add.handle[IO]((_, _) => IO.pure(Right(())))
    assert(List(get, getWithHeaders, getFailing, add).forall(_ ne null))
  }

  test("Reply carries no headers unless asked for") {
    assertEquals(Reply("order").headers, Headers.empty)
    assertEquals(
      Reply("order", Headers("X-Cache" -> "hit")).headers,
      Headers("X-Cache" -> "hit")
    )
    assertEquals(Reply("order").value, "order")
  }
