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

package fs2.nats.jetstream

import munit.FunSuite

class ApiSubjectsSpec extends FunSuite:

  private val default = ApiSubjects("$JS.API.", None)
  private val domain = ApiSubjects("$JS.API.", Some("hub"))

  test("default prefix builds stream subjects") {
    assertEquals(default.accountInfo, "$JS.API.INFO")
    assertEquals(default.streamCreate("ORDERS"), "$JS.API.STREAM.CREATE.ORDERS")
    assertEquals(default.streamUpdate("ORDERS"), "$JS.API.STREAM.UPDATE.ORDERS")
    assertEquals(default.streamInfo("ORDERS"), "$JS.API.STREAM.INFO.ORDERS")
    assertEquals(default.streamDelete("ORDERS"), "$JS.API.STREAM.DELETE.ORDERS")
    assertEquals(default.streamNames, "$JS.API.STREAM.NAMES")
    assertEquals(default.streamList, "$JS.API.STREAM.LIST")
    assertEquals(default.streamPurge("ORDERS"), "$JS.API.STREAM.PURGE.ORDERS")
    assertEquals(default.msgGet("ORDERS"), "$JS.API.STREAM.MSG.GET.ORDERS")
    assertEquals(
      default.msgDelete("ORDERS"),
      "$JS.API.STREAM.MSG.DELETE.ORDERS"
    )
  }

  test("default prefix builds consumer subjects") {
    assertEquals(
      default.consumerCreate("ORDERS", "workers"),
      "$JS.API.CONSUMER.CREATE.ORDERS.workers"
    )
    assertEquals(
      default.consumerCreateFiltered("ORDERS", "workers", "orders.new"),
      "$JS.API.CONSUMER.CREATE.ORDERS.workers.orders.new"
    )
    assertEquals(
      default.consumerInfo("ORDERS", "workers"),
      "$JS.API.CONSUMER.INFO.ORDERS.workers"
    )
    assertEquals(
      default.consumerNames("ORDERS"),
      "$JS.API.CONSUMER.NAMES.ORDERS"
    )
    assertEquals(
      default.msgNext("ORDERS", "workers"),
      "$JS.API.CONSUMER.MSG.NEXT.ORDERS.workers"
    )
  }

  test("domain swaps the prefix") {
    assertEquals(domain.prefix, "$JS.hub.API.")
    assertEquals(domain.streamInfo("ORDERS"), "$JS.hub.API.STREAM.INFO.ORDERS")
    assertEquals(domain.accountInfo, "$JS.hub.API.INFO")
  }

  test("empty domain falls back to apiPrefix") {
    assertEquals(ApiSubjects("$JS.API.", Some("")).prefix, "$JS.API.")
  }
