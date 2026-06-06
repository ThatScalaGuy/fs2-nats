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

/** Integration tests for token authentication.
  *
  * Requires the token-authenticated broker (port 4224), configured to accept
  * exactly one token. Start it with:
  *
  * docker compose up -d nats-token
  */
class TokenAuthIntegrationSpec extends CatsEffectSuite:

  override def munitTimeout: Duration = 60.seconds

  private val natsHost = Host.fromString("localhost").get
  private val natsPort = Port.fromInt(4224).get

  // Matches authorization.token in integration/.../nats-token.conf.
  private val ValidToken = "s3cr3t-test-token"

  private def configFor(token: String) = ClientConfig(
    host = natsHost,
    port = natsPort,
    credentials = Some(NatsCredentials.Token(token)),
    backoff = BackoffConfig.fast.copy(maxRetries = Some(2))
  )

  /** A pub/sub round-trip only completes if the server accepted the CONNECT, so
    * it doubles as the authentication assertion.
    */
  private def roundTrip(client: NatsClient[IO]): IO[String] =
    val subject = s"token.test.${System.currentTimeMillis()}"
    client.subscribe(subject).use { msgStream =>
      for
        _ <- IO.sleep(100.millis)
        _ <- client.publish(subject, Chunk.array("authenticated".getBytes))
        msg <- msgStream.take(1).timeout(5.seconds).compile.lastOrError
      yield msg.payloadAsString
    }

  test("token auth: valid token connects and round-trips") {
    NatsClient
      .connect[IO](configFor(ValidToken))
      .use(roundTrip)
      .map(assertEquals(_, "authenticated"))
      .timeout(15.seconds)
  }

  test("token auth: wrong token cannot complete a round-trip") {
    NatsClient
      .connect[IO](configFor("wrong-token"))
      .use(roundTrip)
      .timeout(10.seconds)
      .attempt
      .map(result =>
        assert(result.isLeft, "wrong token unexpectedly completed a round-trip")
      )
  }
