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

import cats.effect.IO
import com.comcast.ip4s.{ipv4, port}
import com.comcast.ip4s.SocketAddress
import fs2.io.net.Network
import munit.CatsEffectSuite

import java.net.StandardSocketOptions

class TransportConfigSpec extends CatsEffectSuite:

  /** `SocketOption` values compare by reference, so building the default list
    * inline in the case class would silently break equality of two
    * default-constructed configs. Sharing one list keeps `==`/`hashCode` (and
    * therefore preset equality) intact.
    */
  test("default TransportConfigs are equal and carry socket options") {
    assertEquals(TransportConfig(), TransportConfig())
    assert(TransportConfig.default.socketOptions.nonEmpty)
    assertEquals(
      TransportConfig.highThroughput.socketOptions,
      TransportConfig.defaultSocketOptions
    )
    assertEquals(
      TransportConfig.lowLatency.socketOptions,
      TransportConfig.defaultSocketOptions
    )
  }

  /** The point of the defaults is what reaches the kernel, so assert on the
    * dialled socket rather than on the config value. A bound-but-not-accepting
    * listener is enough: the connect completes out of the backlog.
    */
  test("dialling with the default socket options disables Nagle") {
    Network[IO]
      .bind(SocketAddress(ipv4"127.0.0.1", port"0"))
      .flatMap { server =>
        Network[IO].connect(
          server.address,
          TransportConfig.default.socketOptions
        )
      }
      .use(_.getOption(StandardSocketOptions.TCP_NODELAY))
      .map(noDelay => assert(noDelay.exists(_.booleanValue())))
  }
