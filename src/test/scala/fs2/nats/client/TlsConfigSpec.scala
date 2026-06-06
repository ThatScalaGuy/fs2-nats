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
import fs2.nats.errors.NatsError
import munit.CatsEffectSuite

class TlsConfigSpec extends CatsEffectSuite:

  /** When TLS is requested but the caller supplied no TLSContext, the client
    * must fail fast with a clear TlsError rather than silently connecting in
    * plaintext or opening a socket. This is checked at the config layer, so it
    * needs no broker.
    */
  test("connect with useTls=true and no TLSContext fails with TlsError") {
    val config = ClientConfig(
      host = Host.fromString("localhost").get,
      port = Port.fromInt(4222).get,
      useTls = true
    )

    NatsClient
      .connect[IO](config)
      .use(_ => IO.unit)
      .attempt
      .map {
        case Left(_: NatsError.TlsError) => ()
        case other => fail(s"expected TlsError, got $other")
      }
  }
