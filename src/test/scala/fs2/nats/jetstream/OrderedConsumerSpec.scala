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

import fs2.nats.jetstream.OrderedConsumer.Decision

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
