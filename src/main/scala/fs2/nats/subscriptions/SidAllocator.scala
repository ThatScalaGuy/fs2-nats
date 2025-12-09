/*
 * Copyright 2024 fs2-nats contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package fs2.nats.subscriptions

import cats.effect.{Async, Ref}
import cats.syntax.all.*

/**
 * Allocator for subscription IDs (SIDs).
 *
 * SIDs are unique identifiers for subscriptions within a NATS connection.
 * This allocator provides monotonically increasing IDs and tracks which
 * IDs are currently in use for potential reuse in long-running connections.
 *
 * @tparam F The effect type
 */
trait SidAllocator[F[_]]:

  /**
   * Allocate the next available subscription ID.
   *
   * @return The next unique subscription ID
   */
  def next: F[Long]

  /**
   * Release a subscription ID back to the pool.
   * Currently, IDs are not reused but this allows for future optimization.
   *
   * @param sid The subscription ID to release
   * @return Effect that completes when the ID is released
   */
  def release(sid: Long): F[Unit]

  /**
   * Get the current count of active subscriptions.
   *
   * @return The number of active subscriptions
   */
  def activeCount: F[Int]

  /**
   * Get all active subscription IDs.
   *
   * @return Set of all active subscription IDs
   */
  def activeIds: F[Set[Long]]

object SidAllocator:

  /**
   * Create a new SidAllocator.
   *
   * @tparam F The effect type
   * @return A new SidAllocator instance
   */
  def apply[F[_]: Async]: F[SidAllocator[F]] =
    for
      counterRef <- Ref.of[F, Long](0L)
      activeRef <- Ref.of[F, Set[Long]](Set.empty)
    yield new SidAllocator[F]:
      override def next: F[Long] =
        counterRef.modify { current =>
          val nextSid = current + 1
          (nextSid, nextSid)
        }.flatTap(sid => activeRef.update(_ + sid))

      override def release(sid: Long): F[Unit] =
        activeRef.update(_ - sid)

      override def activeCount: F[Int] =
        activeRef.get.map(_.size)

      override def activeIds: F[Set[Long]] =
        activeRef.get
