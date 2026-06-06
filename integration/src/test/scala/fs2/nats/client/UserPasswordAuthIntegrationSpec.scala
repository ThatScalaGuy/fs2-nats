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

/** Integration tests for username/password authentication.
  *
  * Requires the userpass-authenticated broker (port 4225), configured with a
  * single user. Start it with:
  *
  * docker compose up -d nats-userpass
  */
class UserPasswordAuthIntegrationSpec extends CatsEffectSuite:

  override def munitTimeout: Duration = 60.seconds

  private val natsHost = Host.fromString("localhost").get
  private val natsPort = Port.fromInt(4225).get

  // Matches authorization.users in integration/.../nats-userpass.conf.
  private val ValidUser = "myuser"
  private val ValidPass = "mypass"

  private def configFor(user: String, pass: String) = ClientConfig(
    host = natsHost,
    port = natsPort,
    credentials = Some(NatsCredentials.UserPassword(user, pass)),
    backoff = BackoffConfig.fast.copy(maxRetries = Some(2))
  )

  private def roundTrip(client: NatsClient[IO]): IO[String] =
    val subject = s"userpass.test.${System.currentTimeMillis()}"
    client.subscribe(subject).use { msgStream =>
      for
        _ <- IO.sleep(100.millis)
        _ <- client.publish(subject, Chunk.array("authenticated".getBytes))
        msg <- msgStream.take(1).timeout(5.seconds).compile.lastOrError
      yield msg.payloadAsString
    }

  test("userpass auth: valid credentials connect and round-trip") {
    NatsClient
      .connect[IO](configFor(ValidUser, ValidPass))
      .use(roundTrip)
      .map(assertEquals(_, "authenticated"))
      .timeout(15.seconds)
  }

  test("userpass auth: wrong password cannot complete a round-trip") {
    NatsClient
      .connect[IO](configFor(ValidUser, "wrong-password"))
      .use(roundTrip)
      .timeout(10.seconds)
      .attempt
      .map(result =>
        assert(
          result.isLeft,
          "wrong password unexpectedly completed a round-trip"
        )
      )
  }
