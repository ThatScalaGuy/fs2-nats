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

import com.comcast.ip4s.{Host, Port}
import munit.FunSuite

class ServerConfigSpec extends FunSuite:

  private def sa(
      host: String,
      port: Int,
      useTls: Boolean = false
  ): ServerAddress =
    ServerAddress(Host.fromString(host).get, Port.fromInt(port).get, useTls)

  test("seedServers puts the primary first and de-duplicates") {
    val config = ClientConfig(
      host = Host.fromString("a").get,
      port = Port.fromInt(4222).get,
      servers =
        List(sa("a", 4222), sa("b", 5222)) // a:4222 duplicates the primary
    )
    assertEquals(config.seedServers, List(sa("a", 4222), sa("b", 5222)))
  }

  test("fromUrls requires at least one URL") {
    assert(ClientConfig.fromUrls(Nil).isLeft)
  }

  test(
    "fromUrls takes host/port/useTls from the first URL and per-server schemes from the rest"
  ) {
    val result = ClientConfig.fromUrls(List("nats://a:4222", "tls://b:5222"))
    assert(result.isRight, result.toString)
    val config = result.toOption.get
    assertEquals(config.host, Host.fromString("a").get)
    assertEquals(config.port, Port.fromInt(4222).get)
    assertEquals(config.useTls, false) // first URL is nats://
    // the tail URL's tls:// scheme is carried as a per-server useTls
    assertEquals(config.servers, List(sa("b", 5222, useTls = true)))
    assertEquals(
      config.seedServers,
      List(sa("a", 4222, useTls = false), sa("b", 5222, useTls = true))
    )
  }

  test("fromUrls first scheme sets useTls") {
    val config =
      ClientConfig.fromUrls(List("tls://a:4222", "nats://b:5222")).toOption.get
    assertEquals(config.useTls, true)
  }

  test("fromUrls fails on a malformed tail URL") {
    assert(ClientConfig.fromUrls(List("nats://a:4222", "not-a-url")).isLeft)
  }
