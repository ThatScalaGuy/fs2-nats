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

import java.util.{ArrayDeque, HashSet}
import java.util.concurrent.CompletableFuture

/** Flush waiters of one connection. [[track]] registers a waiter before its
  * PING is enqueued; the writer moves it into the wire-ordered `slots` when it
  * serializes the [[Outgoing.Ping]]; each PONG completes the oldest slot.
  * [[failAll]] fails both kinds, and every waiter tracked later fails at once,
  * so no flush waits for its timeout on a connection that is already gone.
  */
private[nats] final class PongSlots:
  private val queued = new HashSet[CompletableFuture[Unit]]()
  private val slots = new ArrayDeque[CompletableFuture[Unit]]()
  private var failed: Throwable = null

  def track(waiter: CompletableFuture[Unit]): Unit =
    val err = synchronized {
      if failed eq null then queued.add(waiter)
      failed
    }
    if err ne null then waiter.completeExceptionally(err)

  /** Undo [[track]] when the PING could not be enqueued. Harmless if it was:
    * [[add]] still moves the waiter into `slots`.
    */
  def untrack(waiter: CompletableFuture[Unit]): Unit =
    synchronized(queued.remove(waiter))

  /** Called by the writer when it serializes the waiter's PING. */
  def add(waiter: CompletableFuture[Unit]): Unit =
    synchronized {
      queued.remove(waiter)
      if failed eq null then slots.addLast(waiter)
    }

  /** Complete the oldest waiter; a PONG with no waiter is ignored. */
  def completeOldest(): Unit =
    val waiter = synchronized(slots.pollFirst())
    if waiter ne null then waiter.complete(())

  def failAll(err: Throwable): Unit =
    val drained = synchronized {
      if failed eq null then failed = err
      val all = new Array[CompletableFuture[Unit]](0)
      val out = slots.toArray(all) ++ queued.toArray(all)
      slots.clear()
      queued.clear()
      out
    }
    drained.foreach(_.completeExceptionally(err))
