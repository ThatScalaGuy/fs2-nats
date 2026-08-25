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

import fs2.nats.micro.ServiceImpl.Counters

class ServiceStatsSpec extends munit.FunSuite:

  private def handler(
      name: String
  ): MicroHandler.Impl[Option, Any, Any, Any, Any] =
    Rpc(name, pattern["svc.op"], Payload.empty, ServiceErr.plain, Payload.empty)
      .handle[Option]((_, _) => Some(Right(())))
      .asInstanceOf[MicroHandler.Impl[Option, Any, Any, Any, Any]]

  test("record increments numRequests and accumulates processing time") {
    val c = Counters.zero.record(100L, None).record(50L, None)
    assertEquals(c.numRequests, 2L)
    assertEquals(c.numErrors, 0L)
    assertEquals(c.processingTimeNanos, 150L)
    assertEquals(c.lastError, None)
  }

  test("record counts an error only when one is present") {
    val failed = Counters.zero.record(10L, Some("500 boom"))
    assertEquals(failed.numRequests, 1L)
    assertEquals(failed.numErrors, 1L)

    val thenOk = failed.record(5L, None)
    assertEquals(thenOk.numRequests, 2L)
    assertEquals(thenOk.numErrors, 1L)
  }

  test("lastError keeps the latest Some over a subsequent None") {
    val afterNone = Counters.zero.record(1L, Some("first")).record(1L, None)
    assertEquals(afterNone.lastError, Some("first"))

    val afterSome = afterNone.record(1L, Some("second"))
    assertEquals(afterSome.lastError, Some("second"))
  }

  test("averageNanos is total/num, truncating, and 0 when empty") {
    assertEquals(Counters.zero.averageNanos, 0L)

    val c = Counters.zero.record(9L, None).record(4L, None)
    assertEquals(c.averageNanos, 6L) // 13 / 2, integer division
  }

  test("zeroCounters seeds every endpoint name with zero counters") {
    val seeded = ServiceImpl.zeroCounters(List(handler("add"), handler("sub")))
    assertEquals(seeded, Map("add" -> Counters.zero, "sub" -> Counters.zero))
  }
