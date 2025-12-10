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

package fs2.nats.client

import munit.CatsEffectSuite
import scala.concurrent.duration.*

class BackoffSpec extends CatsEffectSuite:

  test("exponentialWithJitter returns delay for first attempt") {
    val policy = Backoff.exponentialWithJitter(
      base = 100.millis,
      max = 30.seconds,
      factor = 2.0
    )

    val delay = policy.delay(1)
    assert(delay.isDefined)
    assert(delay.get <= 100.millis)
    assert(delay.get >= Duration.Zero)
  }

  test("exponentialWithJitter increases delay with attempts") {
    val policy = Backoff.exponentialWithJitter(
      base = 100.millis,
      max = 10.seconds,
      factor = 2.0
    )

    var maxSeen = Duration.Zero
    (1 to 10).foreach { attempt =>
      policy.delay(attempt).foreach { d =>
        if d > maxSeen then maxSeen = d
      }
    }

    assert(maxSeen > 100.millis)
  }

  test("exponentialWithJitter respects max delay") {
    val policy = Backoff.exponentialWithJitter(
      base = 1.second,
      max = 2.seconds,
      factor = 10.0
    )

    (1 to 20).foreach { attempt =>
      policy.delay(attempt).foreach { d =>
        assert(d <= 2.seconds, s"Attempt $attempt had delay $d > max 2s")
      }
    }
  }

  test("exponentialWithJitter respects maxRetries") {
    val policy = Backoff.exponentialWithJitter(
      base = 100.millis,
      max = 1.second,
      factor = 2.0,
      maxRetries = Some(3)
    )

    assert(policy.delay(1).isDefined)
    assert(policy.delay(2).isDefined)
    assert(policy.delay(3).isDefined)
    assert(policy.delay(4).isEmpty)
  }

  test("exponentialWithJitter unlimited retries when maxRetries is None") {
    val policy = Backoff.exponentialWithJitter(
      base = 100.millis,
      max = 1.second,
      factor = 2.0,
      maxRetries = None
    )

    assert(policy.delay(100).isDefined)
    assert(policy.delay(1000).isDefined)
  }

  test("fixed returns constant delay") {
    val policy = Backoff.fixed(500.millis)

    assertEquals(policy.delay(1), Some(500.millis))
    assertEquals(policy.delay(10), Some(500.millis))
    assertEquals(policy.delay(100), Some(500.millis))
  }

  test("fixed respects maxRetries") {
    val policy = Backoff.fixed(100.millis, maxRetries = Some(2))

    assert(policy.delay(1).isDefined)
    assert(policy.delay(2).isDefined)
    assert(policy.delay(3).isEmpty)
  }

  test("immediate returns zero delay") {
    val policy = Backoff.immediate(5)

    assertEquals(policy.delay(1), Some(Duration.Zero))
    assertEquals(policy.delay(5), Some(Duration.Zero))
    assert(policy.delay(6).isEmpty)
  }

  test("fromConfig creates policy from BackoffConfig") {
    val config = BackoffConfig(
      baseDelay = 200.millis,
      maxDelay = 5.seconds,
      factor = 1.5,
      maxRetries = Some(10)
    )

    val policy = Backoff.fromConfig(config)

    assert(policy.delay(1).isDefined)
    assert(policy.delay(10).isDefined)
    assert(policy.delay(11).isEmpty)
  }

  test("shouldRetry returns true when delay is available") {
    val policy = Backoff.fixed(100.millis, maxRetries = Some(3))

    assert(policy.shouldRetry(1))
    assert(policy.shouldRetry(3))
    assert(!policy.shouldRetry(4))
  }

  test("decorrelatedJitter varies between attempts") {
    val policy = Backoff.decorrelatedJitter(
      base = 100.millis,
      max = 10.seconds
    )

    val delays = (1 to 10).flatMap(policy.delay)
    assert(delays.nonEmpty)
    assert(delays.forall(_ <= 10.seconds))
  }

class BackoffConfigSpec extends CatsEffectSuite:

  test("default config values") {
    val config = BackoffConfig.default

    assertEquals(config.baseDelay, 100.millis)
    assertEquals(config.maxDelay, 30.seconds)
    assertEquals(config.factor, 2.0)
    assertEquals(config.maxRetries, None)
  }

  test("fast config for testing") {
    val config = BackoffConfig.fast

    assertEquals(config.baseDelay, 10.millis)
    assertEquals(config.maxDelay, 1.second)
  }

  test("conservative config for production") {
    val config = BackoffConfig.conservative

    assertEquals(config.baseDelay, 1.second)
    assertEquals(config.maxDelay, 5.minutes)
  }
