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

package fs2.nats.subscriptions

import cats.effect.IO
import cats.effect.kernel.Ref
import cats.syntax.all.*
import fs2.Chunk
import munit.CatsEffectSuite
import fs2.nats.client.SlowConsumerPolicy
import fs2.nats.protocol.{Headers, NatsFrame}
import scala.concurrent.duration.*

class SubscriptionManagerSpec extends CatsEffectSuite:

  private def createManager(
      queueCapacity: Int = 100,
      policy: SlowConsumerPolicy = SlowConsumerPolicy.Block
  ): IO[(SubscriptionManager[IO], Ref[IO, List[(Long, Option[Int])]])] =
    for
      sidAllocator <- SidAllocator[IO]
      unsubsRef <- Ref.of[IO, List[(Long, Option[Int])]](List.empty)
      manager <- SubscriptionManager[IO](
        queueCapacity,
        policy,
        sidAllocator,
        (sid, maxMsgs) => unsubsRef.update(_ :+ (sid, maxMsgs))
      )
    yield (manager, unsubsRef)

  test("register subscription and receive messages") {
    createManager().flatMap { case (manager, _) =>
      manager.register(1, "test.subject", None).flatMap { result =>
        val (stream, handle) = result
        manager
          .routeMessage(
            NatsFrame
              .MsgFrame("test.subject", 1, None, Chunk.array("Hello".getBytes))
          )
          .flatMap { _ =>
            manager
              .routeMessage(
                NatsFrame.MsgFrame(
                  "test.subject",
                  1,
                  None,
                  Chunk.array("World".getBytes)
                )
              )
              .flatMap { _ =>
                stream.take(2).compile.toList.map { messages =>
                  assertEquals(messages.length, 2)
                  assertEquals(messages(0).payloadAsString, "Hello")
                  assertEquals(messages(1).payloadAsString, "World")
                }
              }
          }
      }
    }
  }

  test("subscription handle provides correct metadata") {
    createManager().flatMap { case (manager, _) =>
      manager.register(42, "foo.bar", Some("workers")).map { result =>
        val (_, handle) = result
        assertEquals(handle.sid, 42L)
        assertEquals(handle.subject, "foo.bar")
        assertEquals(handle.queueGroup, Some("workers"))
      }
    }
  }

  test("unsubscribe removes subscription") {
    createManager().flatMap { case (manager, unsubsRef) =>
      manager.register(1, "test", None).flatMap { result =>
        val (_, handle) = result
        handle.unsubscribe.flatMap { _ =>
          unsubsRef.get.flatMap { unsubs =>
            manager.activeSubscriptions.map { active =>
              assertEquals(unsubs, List((1L, None)))
              assertEquals(active, List.empty)
            }
          }
        }
      }
    }
  }

  test("unsubscribeAfter sends max_msgs") {
    createManager().flatMap { case (manager, unsubsRef) =>
      manager.register(1, "test", None).flatMap { result =>
        val (_, handle) = result
        handle.unsubscribeAfter(5).flatMap { _ =>
          unsubsRef.get.map { unsubs =>
            assertEquals(unsubs, List((1L, Some(5))))
          }
        }
      }
    }
  }

  // test("activeSubscriptions returns all active") {
  // for
  // pair <- createManager()
  // val (manager, _) = pair
  // _ <- manager.register(1, "foo", None)
  // _ <- manager.register(2, "bar", Some("group"))
  // _ <- manager.register(3, "baz", None)

  // active <- manager.activeSubscriptions
  // yield
  // assertEquals(active.length, 3)
  // assert(active.exists(_._2 == "foo"))
  // assert(active.exists(_._2 == "bar"))
  // assert(active.exists(_._2 == "baz"))
  // }

  test("route HMSG frame with headers") {
    createManager().flatMap { case (manager, _) =>
      manager.register(1, "test", None).flatMap { result =>
        val (stream, _) = result
        val headers = Headers("X-Custom" -> "value")
        manager
          .routeMessage(
            NatsFrame.HMsgFrame(
              "test",
              1,
              Some("reply"),
              headers,
              None,
              Chunk.array("data".getBytes)
            )
          )
          .flatMap { _ =>
            stream.take(1).compile.toList.map { messages =>
              val msg = messages.head
              assertEquals(msg.headers.get("X-Custom"), Some("value"))
              assertEquals(msg.replyTo, Some("reply"))
              assertEquals(msg.payloadAsString, "data")
            }
          }
      }
    }
  }

  test("ignore messages for unknown subscription") {
    createManager().flatMap { case (manager, _) =>
      manager
        .routeMessage(
          NatsFrame.MsgFrame("test", 999, None, Chunk.array("orphan".getBytes))
        )
        .map { result =>
          assertEquals(result, None)
        }
    }
  }

  test("closeAll terminates all subscriptions") {
    createManager().flatMap { case (manager, _) =>
      manager.register(1, "foo", None).flatMap { result1 =>
        manager.register(2, "bar", None).flatMap { result2 =>
          val (stream1, _) = result1
          val (stream2, _) = result2
          manager.closeAll.flatMap { _ =>
            manager.activeSubscriptions.map { active =>
              assertEquals(active, List.empty)
            }
          }
        }
      }
    }
  }

  // --- Slow Consumer Policy Tests ---

  test("SlowConsumerPolicy.Block applies backpressure") {
    createManager(queueCapacity = 2, policy = SlowConsumerPolicy.Block)
      .flatMap { case (manager, _) =>
        manager.register(1, "test", None).flatMap { result =>
          val (stream, _) = result
          manager
            .routeMessage(
              NatsFrame.MsgFrame("test", 1, None, Chunk.array("1".getBytes))
            )
            .flatMap { _ =>
              manager
                .routeMessage(
                  NatsFrame.MsgFrame("test", 1, None, Chunk.array("2".getBytes))
                )
                .flatMap { _ =>
                  stream.take(2).compile.toList.map { messages =>
                    assertEquals(messages.length, 2)
                  }
                }
            }
        }
      }
  }

  test("SlowConsumerPolicy.DropNew drops incoming message when full") {
    createManager(queueCapacity = 1, policy = SlowConsumerPolicy.DropNew)
      .flatMap { case (manager, _) =>
        manager.register(1, "test", None).flatMap { result =>
          val (stream, _) = result
          manager
            .routeMessage(
              NatsFrame.MsgFrame("test", 1, None, Chunk.array("1".getBytes))
            )
            .flatMap { _ =>
              manager
                .routeMessage(
                  NatsFrame
                    .MsgFrame("test", 1, None, Chunk.array("dropped".getBytes))
                )
                .flatMap { slowConsumer =>
                  stream.take(1).compile.toList.map { messages =>
                    assert(slowConsumer.isDefined)
                    assertEquals(slowConsumer.get.sid, 1L)
                    assertEquals(messages.head.payloadAsString, "1")
                  }
                }
            }
        }
      }
  }

  test("SlowConsumerPolicy.DropOldest removes oldest when full") {
    createManager(queueCapacity = 1, policy = SlowConsumerPolicy.DropOldest)
      .flatMap { case (manager, _) =>
        manager.register(1, "test", None).flatMap { result =>
          val (stream, _) = result
          manager
            .routeMessage(
              NatsFrame.MsgFrame("test", 1, None, Chunk.array("old".getBytes))
            )
            .flatMap { _ =>
              manager
                .routeMessage(
                  NatsFrame
                    .MsgFrame("test", 1, None, Chunk.array("new".getBytes))
                )
                .flatMap { slowConsumer =>
                  stream.take(1).compile.toList.map { messages =>
                    assert(slowConsumer.isDefined)
                    assertEquals(messages.head.payloadAsString, "new")
                  }
                }
            }
        }
      }
  }

  test("SlowConsumerPolicy.ErrorAndDrop emits event") {
    createManager(queueCapacity = 1, policy = SlowConsumerPolicy.ErrorAndDrop)
      .flatMap { case (manager, _) =>
        manager.register(1, "test", None).flatMap { result =>
          val (stream, _) = result
          manager
            .routeMessage(
              NatsFrame.MsgFrame("test", 1, None, Chunk.array("1".getBytes))
            )
            .flatMap { _ =>
              manager
                .routeMessage(
                  NatsFrame.MsgFrame("test", 1, None, Chunk.array("2".getBytes))
                )
                .map { slowConsumer =>
                  assert(slowConsumer.isDefined)
                  assertEquals(slowConsumer.get.subject, "test")
                  assertEquals(slowConsumer.get.dropped, 1)
                }
            }
        }
      }
  }

class SidAllocatorSpec extends CatsEffectSuite:

  test("allocate sequential IDs") {
    for
      allocator <- SidAllocator[IO]
      id1 <- allocator.next
      id2 <- allocator.next
      id3 <- allocator.next
    yield
      assertEquals(id1, 1L)
      assertEquals(id2, 2L)
      assertEquals(id3, 3L)
  }

  test("track active IDs") {
    for
      allocator <- SidAllocator[IO]
      _ <- allocator.next
      _ <- allocator.next
      count1 <- allocator.activeCount
      ids1 <- allocator.activeIds

      _ <- allocator.release(1L)
      count2 <- allocator.activeCount
      ids2 <- allocator.activeIds
    yield
      assertEquals(count1, 2)
      assertEquals(ids1, Set(1L, 2L))
      assertEquals(count2, 1)
      assertEquals(ids2, Set(2L))
  }

  test("release removes from active set") {
    for
      allocator <- SidAllocator[IO]
      id <- allocator.next
      _ <- allocator.release(id)
      active <- allocator.activeIds
    yield assertEquals(active, Set.empty[Long])
  }

  test("release non-existent ID is safe") {
    for
      allocator <- SidAllocator[IO]
      _ <- allocator.release(999L)
      count <- allocator.activeCount
    yield assertEquals(count, 0)
  }
