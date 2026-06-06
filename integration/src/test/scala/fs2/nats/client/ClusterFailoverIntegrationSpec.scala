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

import cats.effect.{IO, Ref}
import cats.syntax.all.*
import com.comcast.ip4s.{Host, Port}
import fs2.Chunk
import munit.CatsEffectSuite
import scala.concurrent.duration.*
import scala.sys.process.*
import scala.util.Try

/** Cluster failover + discovery tests. These require docker and the three
  * `nats-cluster-{1,2,3}` services from docker-compose.yml:
  *
  * docker compose up -d nats-cluster-1 nats-cluster-2 nats-cluster-3
  *
  * The nodes gossip each other's host-reachable client addresses
  * (`client_advertise=localhost:423N`) into INFO `connect_urls`, so the client
  * can fail over to discovered peers as well as configured seeds.
  */
class ClusterFailoverIntegrationSpec extends CatsEffectSuite:

  override def munitTimeout: Duration = 120.seconds

  private val host = Host.fromString("localhost").get
  private def addr(port: Int): ServerAddress =
    ServerAddress(host, Port.fromInt(port).get)

  private val fastRetry = BackoffConfig.fast.copy(maxRetries = Some(200))

  private def uniqueSubject: String = s"cluster.${System.nanoTime()}"

  private def dockerAvailable: Boolean =
    Try(Seq("docker", "info").!(ProcessLogger(_ => ())) == 0).getOrElse(false)

  private def stopContainer(name: String): IO[Unit] =
    IO.blocking {
      Seq("docker", "stop", "-t", "0", name).!(ProcessLogger(_ => ()))
    }.void

  private def startContainer(name: String): IO[Unit] =
    IO.blocking {
      Seq("docker", "start", name).!(ProcessLogger(_ => ()))
    }.void

  private def healthy(port: Int): IO[Boolean] =
    IO.blocking {
      Try(
        Seq("curl", "-sf", s"http://localhost:$port/healthz")
          .!(ProcessLogger(_ => ())) == 0
      ).getOrElse(false)
    }

  private def waitUntil(cond: IO[Boolean]): IO[Unit] =
    fs2.Stream
      .repeatEval(cond)
      .metered(200.millis)
      .find(identity)
      .compile
      .drain
      .timeout(60.seconds)

  /** Restore a node and wait until it (and the cluster) is healthy again so the
    * next test starts from a whole cluster.
    */
  private def restore(name: String, port: Int): IO[Unit] =
    startContainer(name) *> waitUntil(healthy(port))

  test("fails over to a configured seed when the connected node dies".flaky) {
    assume(dockerAvailable, "docker is not available")
    val config = ClientConfig(
      host = host,
      port = Port.fromInt(4231).get,
      servers = List(addr(4232), addr(4233)),
      noRandomize = true,
      backoff = fastRetry
    )
    NatsClient
      .connect[IO](config)
      .use { client =>
        for
          events <- Ref.of[IO, List[ClientEvent]](Nil)
          _ <- client.events
            .evalTap(e => events.update(_ :+ e))
            .compile
            .drain
            .start
          info0 <- client.serverInfo
          _ <- IO(assertEquals(info0.serverName, Some("n1")))
          // Kill the node we are connected to.
          _ <- stopContainer("nats-cluster-1")
          // Wait for a Reconnected event landing on a different node.
          _ <- waitUntil(
            events.get.map(_.exists {
              case ClientEvent.Reconnected(i, _) =>
                i.serverName.exists(n => n == "n2" || n == "n3")
              case _ => false
            })
          )
          // A fresh round-trip works on the new node.
          subject = uniqueSubject
          received <- client.subscribe(subject).use { stream =>
            IO.sleep(200.millis) *>
              client.publish(subject, Chunk.array("hi".getBytes)) *>
              stream.take(1).timeout(10.seconds).compile.lastOrError
          }
        yield assertEquals(received.payloadAsString, "hi")
      }
      .guarantee(restore("nats-cluster-1", 8231))
  }

  test("fails over to a peer discovered via connect_urls".flaky) {
    assume(dockerAvailable, "docker is not available")
    // Seed ONLY node 1; node 2/3 must be learned from gossip.
    val config = ClientConfig(
      host = host,
      port = Port.fromInt(4231).get,
      noRandomize = true,
      backoff = fastRetry
    )
    NatsClient
      .connect[IO](config)
      .use { client =>
        for
          events <- Ref.of[IO, List[ClientEvent]](Nil)
          _ <- client.events
            .evalTap(e => events.update(_ :+ e))
            .compile
            .drain
            .start
          info0 <- client.serverInfo
          // Discovery: the first INFO already advertises the peers.
          _ <- IO(
            assert(
              info0.connectUrls.exists(_.nonEmpty),
              s"expected discovered connect_urls, got ${info0.connectUrls}"
            )
          )
          _ <- stopContainer("nats-cluster-1")
          // Reconnect must land on a node that was never in the seed config.
          _ <- waitUntil(
            events.get.map(_.exists {
              case ClientEvent.Reconnected(i, _) =>
                i.serverName.exists(n => n == "n2" || n == "n3")
              case _ => false
            })
          )
          subject = uniqueSubject
          received <- client.subscribe(subject).use { stream =>
            IO.sleep(200.millis) *>
              client.publish(subject, Chunk.array("disc".getBytes)) *>
              stream.take(1).timeout(10.seconds).compile.lastOrError
          }
        yield assertEquals(received.payloadAsString, "disc")
      }
      .guarantee(restore("nats-cluster-1", 8231))
  }

  test("buffers a publish issued during the disconnect window".flaky) {
    assume(dockerAvailable, "docker is not available")
    val config = ClientConfig(
      host = host,
      port = Port.fromInt(4231).get,
      servers = List(addr(4232), addr(4233)),
      noRandomize = true,
      backoff = fastRetry
    )
    NatsClient
      .connect[IO](config)
      .use { client =>
        val subject = uniqueSubject
        client.subscribe(subject).use { stream =>
          for
            received <- Ref.of[IO, List[String]](Nil)
            _ <- stream
              .evalTap(m => received.update(_ :+ m.payloadAsString))
              .compile
              .drain
              .start
            events <- Ref.of[IO, List[ClientEvent]](Nil)
            _ <- client.events
              .evalTap(e => events.update(_ :+ e))
              .compile
              .drain
              .start
            _ <- IO.sleep(200.millis)
            _ <- stopContainer("nats-cluster-1")
            // Wait until the client has noticed the disconnect (entered the
            // buffering phase) before publishing.
            _ <- waitUntil(
              events.get.map(_.exists {
                case _: ClientEvent.Disconnected => true
                case _                           => false
              })
            )
            // This publish is buffered (or sent post-reconnect); it must not
            // throw, and must be delivered after failover.
            publishResult <- client
              .publish(subject, Chunk.array("buffered".getBytes))
              .attempt
            _ <- IO(
              assert(
                publishResult.isRight,
                s"publish during disconnect threw: $publishResult"
              )
            )
            _ <- waitUntil(received.get.map(_.contains("buffered")))
          yield ()
        }
      }
      .guarantee(restore("nats-cluster-1", 8231))
  }
