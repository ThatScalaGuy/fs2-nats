/*
 * Copyright 2025 ThatScalaGuy
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package fs2.nats.examples

import cats.effect.{ExitCode, IO, IOApp}
import cats.syntax.all.*
import com.comcast.ip4s.{Host, Port}
import fs2.Chunk
import fs2.nats.client.{ClientConfig, NatsClient}
import fs2.nats.jetstream.*
import fs2.nats.jetstream.protocol.*
import scala.concurrent.duration.*

/** JetStream example: stream management, persistent publish with PubAck, and
  * pull consumption with acks.
  *
  * Prerequisites:
  *   - Start a JetStream-enabled NATS server: docker run -p 4222:4222
  *     nats:latest -js
  *
  * Run with: sbt "runMain fs2.nats.examples.JetStreamExample"
  */
object JetStreamExample extends IOApp:

  override def run(args: List[String]): IO[ExitCode] =
    val config = ClientConfig(
      host = Host.fromString("localhost").get,
      port = Port.fromInt(4222).get
    )

    NatsClient.connect[IO](config).use { client =>
      client.jetStream().use { js =>
        for
          // 1. Create a stream capturing "orders.>".
          _ <- js.addStream(
            StreamConfig(name = "ORDERS", subjects = List("orders.>"))
          )
          _ <- IO.println("Created stream ORDERS")

          // 2. Publish with a PubAck (and dedup via Nats-Msg-Id).
          ack <- js.publish(
            "orders.new",
            Chunk.array("order #1".getBytes),
            opts = PublishOptions(msgId = Some("order-1"))
          )
          _ <- IO.println(s"Published -> stream=${ack.stream} seq=${ack.seq}")

          // 3. Create a durable pull consumer and fetch a batch.
          consumer <- js.createConsumer(
            "ORDERS",
            ConsumerConfig(
              durable = Some("workers"),
              ackPolicy = AckPolicy.Explicit,
              filterSubject = Some("orders.new")
            )
          )
          batch <- consumer.fetch(10, 2.seconds)
          _ <- batch.traverse_ { msg =>
            IO.println(
              s"Fetched seq=${msg.metadata.streamSeq}: ${msg.payloadAsString}"
            ) *> msg.ack
          }

          // 4. Continuous consumption (drain a few, then stop).
          _ <- js.publish("orders.new", Chunk.array("order #2".getBytes))
          _ <- consumer
            .consume(ConsumeOptions(maxMessages = 10, expires = 5.seconds))
            .use { stream =>
              stream
                .evalTap(m =>
                  IO.println(s"Consumed: ${m.payloadAsString}") *> m.ack
                )
                .take(1)
                .compile
                .drain
            }

          _ <- js.deleteStream("ORDERS")
          _ <- IO.println("Done")
        yield ExitCode.Success
      }
    }
