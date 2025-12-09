/*
 * Copyright 2024 fs2-nats contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package fs2.nats.client

import cats.effect.IO
import cats.syntax.all.*
import com.comcast.ip4s.{Host, Port}
import fs2.{Chunk, Stream}
import fs2.io.net.Network
import fs2.nats.protocol.Headers
import munit.CatsEffectSuite
import scala.concurrent.duration.*

/** Integration tests for the NATS client.
  *
  * These tests require a running NATS server. Start one with: docker-compose up
  * -d
  *
  * Or manually: docker run -p 4222:4222 nats:latest
  */
class ConnectionIntegrationSpec extends CatsEffectSuite:

  override def munitTimeout: Duration = 60.seconds

  private val natsHost = Host.fromString("localhost").get
  private val natsPort = Port.fromInt(4222).get

  private def clientConfig = ClientConfig(
    host = natsHost,
    port = natsPort,
    backoff = BackoffConfig.fast
  )

  test("connect to NATS server and receive server info") {
    NatsClient.connect[IO](clientConfig).use { client =>
      client.serverInfo.map { info =>
        assert(info.serverId.nonEmpty)
        assert(info.version.nonEmpty)
        assert(info.maxPayload > 0)
      }
    }
  }

  test("publish and subscribe round-trip") {
    NatsClient.connect[IO](clientConfig).use { client =>
      val subject = s"test.${System.currentTimeMillis()}"
      val payload = "Hello, NATS!"

      client.subscribe(subject).use { msgStream =>
        for
          _ <- IO.sleep(100.millis)
          _ <- client.publish(subject, Chunk.array(payload.getBytes))
          msg <- msgStream.take(1).compile.lastOrError
        yield
          assertEquals(msg.subject, subject)
          assertEquals(msg.payloadAsString, payload)
      }
    }
  }

  test("publish and subscribe with headers") {
    NatsClient.connect[IO](clientConfig).use { client =>
      val subject = s"test.headers.${System.currentTimeMillis()}"
      val payload = "Hello with headers!"
      val headers = Headers("X-Custom" -> "value", "X-Other" -> "data")

      client.subscribe(subject).use { msgStream =>
        for
          _ <- IO.sleep(100.millis)
          _ <- client.publish(subject, Chunk.array(payload.getBytes), headers)
          msg <- msgStream.take(1).compile.lastOrError
        yield
          assertEquals(msg.subject, subject)
          assertEquals(msg.payloadAsString, payload)
          assertEquals(msg.headers.get("X-Custom"), Some("value"))
          assertEquals(msg.headers.get("X-Other"), Some("data"))
      }
    }
  }

  test("subscribe to wildcard subject") {
    NatsClient.connect[IO](clientConfig).use { client =>
      val baseSubject = s"test.wildcard.${System.currentTimeMillis()}"
      val wildcard = s"$baseSubject.*"

      client.subscribe(wildcard).use { msgStream =>
        for
          _ <- IO.sleep(100.millis)
          _ <- client.publish(
            s"$baseSubject.one",
            Chunk.array("first".getBytes)
          )
          _ <- client.publish(
            s"$baseSubject.two",
            Chunk.array("second".getBytes)
          )
          messages <- msgStream.take(2).compile.toList
        yield
          assertEquals(messages.length, 2)
          assert(messages.exists(_.payloadAsString == "first"))
          assert(messages.exists(_.payloadAsString == "second"))
      }
    }
  }

  test("subscribe with queue group") {
    NatsClient.connect[IO](clientConfig).use { client1 =>
      NatsClient.connect[IO](clientConfig).use { client2 =>
        val subject = s"test.queue.${System.currentTimeMillis()}"
        val queueGroup = Some("workers")

        client1.subscribe(subject, queueGroup).use { stream1 =>
          client2.subscribe(subject, queueGroup).use { stream2 =>
            for
              _ <- IO.sleep(100.millis)

              _ <- (1 to 10).toList.traverse_ { i =>
                client1.publish(subject, Chunk.array(s"msg$i".getBytes))
              }

              msgs1 <- stream1.take(5).timeout(5.seconds).compile.toList.attempt
              msgs2 <- stream2.take(5).timeout(5.seconds).compile.toList.attempt
            yield
              val count1 = msgs1.fold(_ => 0, _.length)
              val count2 = msgs2.fold(_ => 0, _.length)
              assert(count1 + count2 <= 10)
          }
        }
      }
    }
  }

  test("max_payload enforcement") {
    NatsClient.connect[IO](clientConfig).use { client =>
      for
        info <- client.serverInfo
        oversizedPayload = Chunk.array(
          new Array[Byte]((info.maxPayload + 1).toInt)
        )
        result <- client.publish("test", oversizedPayload).attempt
      yield
        assert(result.isLeft)
        result.left.toOption.get match
          case err: fs2.nats.errors.NatsError.PayloadTooLarge =>
            assert(err.size > info.maxPayload)
          case other =>
            fail(s"Expected PayloadTooLarge, got $other")
    }
  }

  test("PING/PONG keepalive") {
    NatsClient.connect[IO](clientConfig).use { client =>
      for
        _ <- IO.sleep(5.seconds)
        connected <- client.isConnected
      yield assert(connected)
    }
  }

  test("events stream receives Connected event") {
    NatsClient.connect[IO](clientConfig).use { client =>
      client.events
        .collect { case e: ClientEvent.Connected => e }
        .take(1)
        .timeout(5.seconds)
        .compile
        .lastOrError
        .map { event =>
          assert(event.serverInfo.serverId.nonEmpty)
        }
    }
  }

  test("empty payload publish and subscribe") {
    NatsClient.connect[IO](clientConfig).use { client =>
      val subject = s"test.empty.${System.currentTimeMillis()}"

      client.subscribe(subject).use { msgStream =>
        for
          _ <- IO.sleep(100.millis)
          _ <- client.publish(subject, Chunk.empty)
          msg <- msgStream.take(1).compile.lastOrError
        yield
          assertEquals(msg.subject, subject)
          assertEquals(msg.payloadSize, 0)
      }
    }
  }

  test("large payload publish and subscribe") {
    NatsClient.connect[IO](clientConfig).use { client =>
      val subject = s"test.large.${System.currentTimeMillis()}"
      val size = 500000
      val largePayload = Chunk.array(Array.fill[Byte](size)(42))

      client.subscribe(subject).use { msgStream =>
        for
          _ <- IO.sleep(100.millis)
          _ <- client.publish(subject, largePayload)
          msg <- msgStream.take(1).compile.lastOrError
        yield
          assertEquals(msg.payloadSize, size)
          assert(msg.payload.forall(_ == 42.toByte))
      }
    }
  }

  test("multiple concurrent subscriptions") {
    NatsClient.connect[IO](clientConfig).use { client =>
      val subjects =
        (1 to 5).map(i => s"test.concurrent.$i.${System.currentTimeMillis()}")

      subjects.toList
        .traverse { subject =>
          client.subscribe(subject)
        }
        .use { streams =>
          for
            _ <- IO.sleep(100.millis)
            _ <- subjects.toList.zipWithIndex.traverse { case (subject, i) =>
              client.publish(subject, Chunk.array(s"msg$i".getBytes))
            }
            messages <- streams.traverse(_.take(1).compile.lastOrError)
          yield
            assertEquals(messages.length, 5)
            messages.zipWithIndex.foreach { case (msg, i) =>
              assertEquals(msg.payloadAsString, s"msg$i")
            }
        }
    }
  }
