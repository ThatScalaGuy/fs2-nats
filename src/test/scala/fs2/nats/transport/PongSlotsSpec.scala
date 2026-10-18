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

package fs2.nats.transport

import java.util.concurrent.{CompletableFuture, ExecutionException}

class PongSlotsSpec extends munit.FunSuite:

  private def cause(f: CompletableFuture[Unit]): Throwable =
    intercept[ExecutionException](f.get()).getCause

  test("each PONG completes the oldest waiter") {
    val slots = new PongSlots
    val (a, b) = (new CompletableFuture[Unit], new CompletableFuture[Unit])
    slots.add(a); slots.add(b)
    slots.completeOldest()
    assert(a.isDone && !b.isDone)
    slots.completeOldest()
    assert(b.isDone)
    slots.completeOldest() // no waiter left: ignored
  }

  test("failAll fails written, queued and later waiters") {
    val slots = new PongSlots
    val Seq(written, queued, later) = Seq.fill(3)(new CompletableFuture[Unit])
    val err = new RuntimeException("lost")
    slots.track(written)
    slots.add(written)
    slots.track(queued) // PING never serialized
    slots.failAll(err)
    slots.track(later)
    assertEquals(cause(written), err)
    assertEquals(cause(queued), err)
    assertEquals(cause(later), err)
  }

  test("an untracked waiter is not failed by failAll") {
    val slots = new PongSlots
    val w = new CompletableFuture[Unit]
    slots.track(w)
    slots.untrack(w) // its PING never made it into the write queue
    slots.failAll(new RuntimeException("lost"))
    assert(!w.isDone)
  }
