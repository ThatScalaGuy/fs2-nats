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

import fs2.nats.jetstream.OrderedConsumer.{Decision, Step}

class OrderedConsumerSpec extends munit.FunSuite:

  test("in-order delivery emits"):
    assertEquals(
      OrderedConsumer.decide(
        expectedConsumerSeq = 1L,
        lastStreamSeq = 0L,
        msgConsumerSeq = 1L
      ),
      Decision.Emit
    )
    assertEquals(
      OrderedConsumer.decide(
        expectedConsumerSeq = 42L,
        lastStreamSeq = 99L,
        msgConsumerSeq = 42L
      ),
      Decision.Emit
    )

  test(
    "a gap (consumerSeq ahead of expected) recreates from lastStreamSeq + 1"
  ):
    assertEquals(
      OrderedConsumer.decide(
        expectedConsumerSeq = 6L,
        lastStreamSeq = 100L,
        msgConsumerSeq = 7L
      ),
      Decision.Recreate(101L)
    )
    // A larger jump still recreates from the last in-order stream seq + 1.
    assertEquals(
      OrderedConsumer.decide(
        expectedConsumerSeq = 2L,
        lastStreamSeq = 5L,
        msgConsumerSeq = 50L
      ),
      Decision.Recreate(6L)
    )

  test("recreate from sequence 1 when no message has been delivered yet"):
    assertEquals(
      OrderedConsumer.decide(
        expectedConsumerSeq = 1L,
        lastStreamSeq = 0L,
        msgConsumerSeq = 3L
      ),
      Decision.Recreate(1L)
    )

  test("a stale/duplicate delivery (consumerSeq behind expected) is dropped"):
    assertEquals(
      OrderedConsumer.decide(
        expectedConsumerSeq = 10L,
        lastStreamSeq = 200L,
        msgConsumerSeq = 4L
      ),
      Decision.DropStale
    )
    assertEquals(
      OrderedConsumer.decide(
        expectedConsumerSeq = 2L,
        lastStreamSeq = 1L,
        msgConsumerSeq = 1L
      ),
      Decision.DropStale
    )

  // ---- step: the whole per-message transition, including the cycle-name
  // filter that `decide` never sees.

  private val st0 = OrderedState(
    expectedConsumerSeq = 5L,
    lastStreamSeq = 100L,
    cycle = 3L,
    consumer = "C1"
  )

  test("a delivery naming a superseded consumer is dropped, state untouched"):
    // In order for the *old* cycle, so it would emit if the name were current.
    assertEquals(
      OrderedConsumer.step(st0, "C0", msgConsumerSeq = 5L, msgStreamSeq = 101L),
      (st0, Step.Drop)
    )
    // Ahead of expected, so it would recreate if the name were current — a
    // message from a dead cycle must not trigger another recreate.
    assertEquals(
      OrderedConsumer.step(st0, "C0", msgConsumerSeq = 9L, msgStreamSeq = 105L),
      (st0, Step.Drop)
    )

  test("every delivery is dropped before the first create publishes a name"):
    val fresh = OrderedState(1L, 0L, 0L, "")
    assertEquals(
      OrderedConsumer.step(fresh, "C1", 1L, 1L),
      (fresh, Step.Drop)
    )

  test("an in-order delivery advances expected by 1 and records streamSeq"):
    assertEquals(
      OrderedConsumer.step(st0, "C1", msgConsumerSeq = 5L, msgStreamSeq = 101L),
      (st0.copy(expectedConsumerSeq = 6L, lastStreamSeq = 101L), Step.Deliver)
    )

  test("a gap recreates without mutating the state"):
    // `recreate` re-reads the state to derive its resume sequence, so the gap
    // transition must leave `lastStreamSeq` exactly as it was.
    assertEquals(
      OrderedConsumer.step(st0, "C1", msgConsumerSeq = 7L, msgStreamSeq = 103L),
      (st0, Step.Recreate)
    )

  test("a stale delivery is dropped without mutating the state"):
    assertEquals(
      OrderedConsumer.step(st0, "C1", msgConsumerSeq = 2L, msgStreamSeq = 98L),
      (st0, Step.Drop)
    )

  test("folding step over an in-order run tracks both sequences"):
    val (finalSt, steps) =
      (1 to 20).foldLeft((OrderedState(1L, 0L, 0L, "C1"), List.empty[Step])) {
        case ((st, acc), i) =>
          val (next, step) = OrderedConsumer.step(st, "C1", i.toLong, i.toLong)
          (next, step :: acc)
      }
    assertEquals(finalSt, OrderedState(21L, 20L, 0L, "C1"))
    assertEquals(steps.distinct, List(Step.Deliver))
