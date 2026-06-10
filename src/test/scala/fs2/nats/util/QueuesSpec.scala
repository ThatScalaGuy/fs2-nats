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

package fs2.nats.util

import scala.concurrent.duration.*

import cats.effect.IO
import cats.syntax.all.*
import munit.CatsEffectSuite

class QueuesSpec extends CatsEffectSuite:

  test("large queue preserves FIFO order") {
    for
      q <- Queues.largeBounded[IO, Int](4)
      _ <- List(1, 2, 3).traverse_(q.offer)
      a <- q.take
      b <- q.take
      c <- q.take
    yield assertEquals((a, b, c), (1, 2, 3))
  }

  test("tryOffer fails at capacity and succeeds again after a take") {
    for
      q <- Queues.largeBounded[IO, Int](2)
      _ <- q.offer(1) *> q.offer(2)
      full <- q.tryOffer(3)
      _ <- q.take
      after <- q.tryOffer(3)
      size <- q.size
    yield
      assertEquals(full, false)
      assertEquals(after, true)
      assertEquals(size, 2)
  }

  test("offer blocks when full and resumes after a take") {
    for
      q <- Queues.largeBounded[IO, Int](2)
      _ <- q.offer(1) *> q.offer(2)
      fiber <- q.offer(3).start
      _ <- IO.sleep(50.millis)
      stalled <- q.size.map(_ == 2)
      first <- q.take
      _ <- fiber.joinWithNever
      rest <- (q.take, q.take).tupled
    yield
      assert(stalled, "offer must not complete while the queue is full")
      assertEquals(first, 1)
      assertEquals(rest, (2, 3))
  }

  test("blocked offer is cancelable") {
    for
      q <- Queues.largeBounded[IO, Int](1)
      _ <- q.offer(1)
      fiber <- q.offer(2).start
      _ <- IO.sleep(50.millis)
      _ <- fiber.cancel
      size <- q.size
    yield assertEquals(size, 1)
  }

  test("a take after a canceled blocked offer still wakes later offerers") {
    for
      q <- Queues.largeBounded[IO, Int](1)
      _ <- q.offer(1)
      doomed <- q.offer(2).start
      _ <- IO.sleep(20.millis)
      _ <- doomed.cancel
      fiber <- q.offer(3).start
      _ <- IO.sleep(20.millis)
      a <- q.take
      _ <- fiber.joinWithNever
      b <- q.take
    yield assertEquals((a, b), (1, 3))
  }

  test("tryTakeN frees capacity in bulk") {
    for
      q <- Queues.largeBounded[IO, Int](3)
      _ <- List(1, 2, 3).traverse_(q.offer)
      xs <- q.tryTakeN(None)
      size <- q.size
      again <- List(4, 5, 6).traverse(q.tryOffer)
      over <- q.tryOffer(7)
    yield
      assertEquals(xs, List(1, 2, 3))
      assertEquals(size, 0)
      assertEquals(again, List(true, true, true))
      assertEquals(over, false)
  }

  test("multiple blocked offerers all complete as the consumer drains") {
    for
      q <- Queues.largeBounded[IO, Int](2)
      _ <- q.offer(0) *> q.offer(1)
      fibers <- (2 to 6).toList.traverse(i => q.offer(i).start)
      taken <- q.take.replicateA(7)
      _ <- fibers.traverse_(_.joinWithNever)
    yield assertEquals(taken.toSet, (0 to 6).toSet)
  }

  test("single-producer single-consumer stress keeps order and bound") {
    val n = 10000
    for
      q <- Queues.largeBounded[IO, Int](8)
      consumer <- (0 until n).toList.traverse(_ => q.take).start
      _ <- (0 until n).toList.traverse_(q.offer)
      taken <- consumer.joinWithNever
    yield assertEquals(taken, (0 until n).toList)
  }

  test("bounded dispatches large capacities to the external-count queue") {
    for
      q <- Queues.bounded[IO, Int](Queues.FastPathLimit)
      _ <- q.offer(42)
      size <- q.size
      a <- q.take
    yield
      assertEquals(size, 1)
      assertEquals(a, 42)
  }

  test("bounded keeps cats-effect semantics for small capacities") {
    for
      q <- Queues.bounded[IO, Int](2)
      _ <- q.offer(1) *> q.offer(2)
      full <- q.tryOffer(3)
      a <- q.take
    yield
      assertEquals(full, false)
      assertEquals(a, 1)
  }
