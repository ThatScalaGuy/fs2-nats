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

class PushStatusSpec extends FunSuite:

  test("data messages have no status") {
    assertEquals(
      PushStatus.classify(None, Some("$JS.ACK.x"), None),
      PushSignal.Data
    )
  }

  test("100 without a reply is an idle heartbeat") {
    assertEquals(
      PushStatus.classify(Some(100), None, Some("Idle Heartbeat")),
      PushSignal.Heartbeat
    )
    // empty reply also counts as heartbeat
    assertEquals(
      PushStatus.classify(Some(100), Some(""), None),
      PushSignal.Heartbeat
    )
  }

  test("100 with a reply is flow control carrying the echo target") {
    assertEquals(
      PushStatus.classify(Some(100), Some("_INBOX.fc.1"), Some("FlowControl")),
      PushSignal.FlowControl("_INBOX.fc.1")
    )
  }

  test("other statuses fail the push") {
    PushStatus.classify(Some(409), None, Some("Consumer Deleted")) match
      case PushSignal.Fail(409, _) => ()
      case other                   => fail(s"expected Fail(409), got $other")
  }
