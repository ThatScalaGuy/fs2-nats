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

import fs2.nats.jetstream.protocol.ConsumerConfig
import munit.FunSuite

class ConsumerCreateSpec extends FunSuite:

  private val subjects = ApiSubjects("$JS.API.", None)

  test("durable consumer uses the named create subject") {
    val cfg = ConsumerConfig(durable = Some("workers"))
    assertEquals(
      ConsumerCreate.subject(subjects, "ORDERS", cfg),
      "$JS.API.CONSUMER.CREATE.ORDERS.workers"
    )
  }

  test("durable with a single filterSubject uses the filtered variant") {
    val cfg =
      ConsumerConfig(
        durable = Some("workers"),
        filterSubject = Some("orders.new")
      )
    assertEquals(
      ConsumerCreate.subject(subjects, "ORDERS", cfg),
      "$JS.API.CONSUMER.CREATE.ORDERS.workers.orders.new"
    )
  }

  test("filterSubjects (plural) suppresses the filtered subject variant") {
    val cfg = ConsumerConfig(
      durable = Some("workers"),
      filterSubjects = List("orders.new", "orders.old")
    )
    assertEquals(
      ConsumerCreate.subject(subjects, "ORDERS", cfg),
      "$JS.API.CONSUMER.CREATE.ORDERS.workers"
    )
  }

  test("explicit name (non-durable) uses the named create subject") {
    val cfg = ConsumerConfig(name = Some("ephem-named"))
    assertEquals(
      ConsumerCreate.subject(subjects, "ORDERS", cfg),
      "$JS.API.CONSUMER.CREATE.ORDERS.ephem-named"
    )
  }

  test("no durable and no name uses the ephemeral subject") {
    val cfg = ConsumerConfig(filterSubject = Some("orders.new"))
    assertEquals(
      ConsumerCreate.subject(subjects, "ORDERS", cfg),
      "$JS.API.CONSUMER.CREATE.ORDERS"
    )
  }
