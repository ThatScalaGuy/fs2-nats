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
import fs2.io.file.Path
import munit.CatsEffectSuite
import scala.concurrent.duration.*

/** Integration tests for decentralized JWT (`.creds`) authentication.
  *
  * Requires the operator-mode broker with a MEMORY resolver (port 4226). It
  * trusts the operator whose account/user JWTs are preloaded in nats-creds.conf
  * and which issued testuser.creds. Start it with:
  *
  * docker compose up -d nats-creds
  *
  * This exercises the whole decentralized path end-to-end: load the .creds file
  * -> Creds.parse -> NKey(seed, Some(jwt)) -> CONNECT sends jwt + sig -> the
  * server validates the JWT against the preloaded account and verifies the
  * nonce signature.
  */
class CredsAuthIntegrationSpec extends CatsEffectSuite:

  override def munitTimeout: Duration = 60.seconds

  private val natsHost = Host.fromString("localhost").get
  private val natsPort = Port.fromInt(4226).get

  // Resolved relative to the forked integration module base directory.
  private val credsPath = Path("src/test/resources/testuser.creds")

  // A valid but unrelated seed (from NKeyAuthIntegrationSpec); pairing it with
  // the real JWT yields a signature that will not verify against the JWT's
  // subject key, so the server must reject it.
  private val UnrelatedSeed =
    "SUAIKKMDHPRHZKXNXCF7M7Q6UK2KVGC7R32PMYCYXEQZDKKLBQGQKJV5AQ"

  private def configFor(creds: NatsCredentials) = ClientConfig(
    host = natsHost,
    port = natsPort,
    credentials = Some(creds),
    backoff = BackoffConfig.fast.copy(maxRetries = Some(2))
  )

  private def roundTrip(client: NatsClient[IO]): IO[String] =
    val subject = s"creds.test.${System.currentTimeMillis()}"
    client.subscribe(subject).use { msgStream =>
      for
        _ <- IO.sleep(100.millis)
        _ <- client.publish(subject, Chunk.array("authenticated".getBytes))
        msg <- msgStream.take(1).timeout(5.seconds).compile.lastOrError
      yield msg.payloadAsString
    }

  test("creds auth: loading testuser.creds connects and round-trips") {
    NatsCredentials
      .fromCredsFile[IO](credsPath)
      .flatMap(creds => NatsClient.connect[IO](configFor(creds)).use(roundTrip))
      .map(assertEquals(_, "authenticated"))
      .timeout(15.seconds)
  }

  test("creds auth: a JWT signed with the wrong seed is rejected") {
    NatsCredentials
      .fromCredsFile[IO](credsPath)
      .map {
        case NatsCredentials.NKey(_, Some(jwt)) =>
          NatsCredentials.NKey(UnrelatedSeed, Some(jwt))
        case other =>
          fail(s"expected NKey credentials from the creds file, got $other")
      }
      .flatMap(bad => NatsClient.connect[IO](configFor(bad)).use(roundTrip))
      .timeout(10.seconds)
      .attempt
      .map(result =>
        assert(
          result.isLeft,
          "a JWT with a mismatched signing seed unexpectedly authenticated"
        )
      )
  }
