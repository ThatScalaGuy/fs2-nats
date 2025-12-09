/*
 * Copyright 2024 fs2-nats contributors
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
import fs2.{Chunk, Stream}
import fs2.nats.client.{ClientConfig, ClientEvent, NatsClient}
import fs2.nats.protocol.Headers
import scala.concurrent.duration.*

/**
 * Basic example demonstrating fs2-nats usage.
 *
 * Prerequisites:
 * - Start a NATS server: docker run -p 4222:4222 nats:latest
 *
 * Run with:
 *   sbt "runMain fs2.nats.examples.Basic"
 */
object Basic extends IOApp:

  override def run(args: List[String]): IO[ExitCode] =
    val config = ClientConfig(
      host = Host.fromString("localhost").get,
      port = Port.fromInt(4222).get
    )

    NatsClient.connect[IO](config).use { client =>
      for
        _ <- IO.println("Connected to NATS!")

        info <- client.serverInfo
        _ <- IO.println(s"Server: ${info.serverId} v${info.version}")
        _ <- IO.println(s"Max payload: ${info.maxPayload} bytes")

        _ <- simplePublishSubscribe(client)
        _ <- headersExample(client)
        _ <- wildcardExample(client)
        _ <- eventsExample(client)

        _ <- IO.println("\nExamples completed successfully!")
      yield ExitCode.Success
    }.handleErrorWith { err =>
      IO.println(s"Error: ${err.getMessage}").as(ExitCode.Error)
    }

  private def simplePublishSubscribe(client: NatsClient[IO]): IO[Unit] =
    IO.println("\n--- Simple Publish/Subscribe ---") *>
      client.subscribe("example.hello").use { messages =>
        for
          _ <- IO.sleep(100.millis)

          _ <- client.publish(
            "example.hello",
            Chunk.array("Hello, NATS!".getBytes)
          )

          msg <- messages.take(1).compile.lastOrError

          _ <- IO.println(s"Received: ${msg.payloadAsString}")
          _ <- IO.println(s"Subject: ${msg.subject}")
        yield ()
      }

  private def headersExample(client: NatsClient[IO]): IO[Unit] =
    IO.println("\n--- Publishing with Headers ---") *>
      client.subscribe("example.headers").use { messages =>
        for
          _ <- IO.sleep(100.millis)

          headers = Headers(
            "X-Request-Id" -> "abc123",
            "X-Timestamp" -> System.currentTimeMillis().toString
          )

          _ <- client.publish(
            "example.headers",
            Chunk.array("Message with headers".getBytes),
            headers
          )

          msg <- messages.take(1).compile.lastOrError

          _ <- IO.println(s"Received: ${msg.payloadAsString}")
          _ <- IO.println(s"Headers: ${msg.headers.entries.toList}")
        yield ()
      }

  private def wildcardExample(client: NatsClient[IO]): IO[Unit] =
    IO.println("\n--- Wildcard Subscriptions ---") *>
      client.subscribe("example.events.*").use { messages =>
        for
          _ <- IO.sleep(100.millis)

          _ <- client.publish("example.events.created", Chunk.array("created".getBytes))
          _ <- client.publish("example.events.updated", Chunk.array("updated".getBytes))
          _ <- client.publish("example.events.deleted", Chunk.array("deleted".getBytes))

          received <- messages.take(3).compile.toList

          _ <- IO.println(s"Received ${received.length} messages:")
          _ <- received.traverse_ { msg =>
            IO.println(s"  ${msg.subject}: ${msg.payloadAsString}")
          }
        yield ()
      }

  private def eventsExample(client: NatsClient[IO]): IO[Unit] =
    IO.println("\n--- Client Events ---") *>
      client.events
        .take(2)
        .timeout(2.seconds)
        .compile
        .toList
        .attempt
        .flatMap {
          case Right(events) =>
            IO.println(s"Observed ${events.length} events:") *>
              events.traverse_ { event =>
                IO.println(s"  $event")
              }
          case Left(_) =>
            IO.println("  (no additional events observed)")
        }

/**
 * Example demonstrating request/reply pattern.
 */
object RequestReplyExample extends IOApp:

  override def run(args: List[String]): IO[ExitCode] =
    val config = ClientConfig.localhost()

    NatsClient.connect[IO](config).use { client =>
      for
        _ <- IO.println("Request/Reply Example")

        _ <- client.subscribe("service.echo").use { requests =>
          val processor = requests
            .take(3)
            .evalMap { request =>
              request.replyTo match
                case Some(reply) =>
                  val response = s"Echo: ${request.payloadAsString}"
                  client.publish(reply, Chunk.array(response.getBytes))
                case None =>
                  IO.println("Request without reply-to, ignoring")
            }
            .compile
            .drain

          val sender = IO.sleep(100.millis) *>
            (1 to 3).toList.traverse_ { i =>
              IO.println(s"Sending request $i") *>
                client.publish(
                  "service.echo",
                  Chunk.array(s"Request $i".getBytes),
                  Headers.empty,
                  Some(s"_INBOX.$i")
                )
            }

          processor.race(sender).void
        }

        _ <- IO.println("Done!")
      yield ExitCode.Success
    }.handleErrorWith { err =>
      IO.println(s"Error: ${err.getMessage}").as(ExitCode.Error)
    }

/**
 * Example demonstrating queue groups for load balancing.
 */
object QueueGroupExample extends IOApp:

  override def run(args: List[String]): IO[ExitCode] =
    val config = ClientConfig.localhost()

    IO.println("Queue Group Example (load balancing)") *>
      IO.println("Starting 3 workers...") *>
      (
        NatsClient.connect[IO](config),
        NatsClient.connect[IO](config),
        NatsClient.connect[IO](config)
      ).tupled.use { case (client1, client2, client3) =>
        val subject = "work.queue"
        val queueGroup = Some("workers")

        def worker(name: String, client: NatsClient[IO]): Stream[IO, Unit] =
          Stream.resource(client.subscribe(subject, queueGroup)).flatMap { messages =>
            messages.evalMap { msg =>
              IO.println(s"  $name processed: ${msg.payloadAsString}")
            }
          }

        val workers = worker("Worker-1", client1)
          .merge(worker("Worker-2", client2))
          .merge(worker("Worker-3", client3))

        val producer = Stream.sleep[IO](200.millis) ++
          Stream.emits(1 to 10).evalMap { i =>
            IO.println(s"Publishing job $i") *>
              client1.publish(subject, Chunk.array(s"Job-$i".getBytes))
          }

        workers
          .concurrently(producer)
          .take(10)
          .timeout(10.seconds)
          .compile
          .drain
          .as(ExitCode.Success)
      }
      .handleErrorWith { err =>
        IO.println(s"Error: ${err.getMessage}").as(ExitCode.Error)
      }
