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

import cats.effect.IO
import com.comcast.ip4s.{Host, Port}
import fs2.Chunk
import munit.CatsEffectSuite
import scala.concurrent.duration.*

/** Integration tests for NKey (Ed25519) authentication.
  *
  * These require the dedicated nkey-authenticated broker, which is configured
  * to accept exactly one user (the public key derived from `AuthorizedSeed`).
  * Start it with:
  *
  * docker compose up -d nats-nkey
  *
  * It listens on port 4223 so the plaintext broker on 4222 is unaffected.
  */
class NKeyAuthIntegrationSpec extends CatsEffectSuite:

  override def munitTimeout: Duration = 60.seconds

  private val natsHost = Host.fromString("localhost").get
  private val natsPort = Port.fromInt(4223).get

  // The seed whose public key is whitelisted in integration/.../nats-nkey.conf.
  private val AuthorizedSeed =
    "SUAO7NFVXBKDTBLB74TVJUV4H2BGV6YJYAIAQGFROYC3FWWLBGRHG6YHPQ"

  // A valid but unauthorized seed (different public key, not in the server's
  // user list). Generated with `nsc generate nkey --user`.
  private val UnauthorizedSeed =
    "SUAIKKMDHPRHZKXNXCF7M7Q6UK2KVGC7R32PMYCYXEQZDKKLBQGQKJV5AQ"

  private def configFor(seed: String) = ClientConfig(
    host = natsHost,
    port = natsPort,
    credentials = Some(NatsCredentials.NKey(seed)),
    backoff = BackoffConfig.fast.copy(maxRetries = Some(2))
  )

  /** A pub/sub round-trip only completes if the server accepted our signed
    * CONNECT, so it doubles as the authentication assertion.
    */
  private def roundTrip(client: NatsClient[IO]): IO[String] =
    val subject = s"nkey.test.${System.currentTimeMillis()}"
    val payload = "authenticated"
    client.subscribe(subject).use { msgStream =>
      for
        _ <- IO.sleep(100.millis)
        _ <- client.publish(subject, Chunk.array(payload.getBytes))
        msg <- msgStream.take(1).timeout(5.seconds).compile.lastOrError
      yield msg.payloadAsString
    }

  test("NKey auth: authorized seed signs the nonce and connects") {
    NatsClient
      .connect[IO](configFor(AuthorizedSeed))
      .use(roundTrip)
      .map(assertEquals(_, "authenticated"))
      .timeout(15.seconds)
  }

  test("NKey auth: unauthorized seed cannot complete a round-trip") {
    NatsClient
      .connect[IO](configFor(UnauthorizedSeed))
      .use(roundTrip)
      .timeout(10.seconds)
      .attempt
      .map(result =>
        assert(
          result.isLeft,
          "unauthorized NKey unexpectedly completed a round-trip"
        )
      )
  }
