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

import cats.effect.Async
import cats.syntax.all.*
import scala.concurrent.duration.*
import scala.util.Random

/**
 * Backoff policy for calculating delays between retry attempts.
 * Immutable and thread-safe.
 */
trait BackoffPolicy:

  /**
   * Calculate the delay for a given retry attempt.
   *
   * @param attempt The attempt number (1-based)
   * @return The delay before this attempt, or None if max retries exceeded
   */
  def delay(attempt: Int): Option[FiniteDuration]

  /**
   * Check if more retries are allowed.
   *
   * @param attempt The current attempt number
   * @return True if another retry is allowed
   */
  def shouldRetry(attempt: Int): Boolean = delay(attempt).isDefined

/**
 * Backoff policy implementations with jitter for distributed systems.
 */
object Backoff:

  /**
   * Create an exponential backoff policy with full jitter.
   *
   * Full jitter adds randomness uniformly distributed between 0 and the
   * calculated delay, which helps decorrelate retry storms in distributed systems.
   *
   * Delay calculation: min(maxDelay, baseDelay * factor^(attempt-1)) * random(0, 1)
   *
   * @param base The initial delay before first retry
   * @param max The maximum delay between retries
   * @param factor The multiplier applied to delay after each retry
   * @param maxRetries Optional maximum number of retry attempts (None for unlimited)
   * @return A BackoffPolicy implementing exponential backoff with full jitter
   */
  def exponentialWithJitter(
      base: FiniteDuration = 100.millis,
      max: FiniteDuration = 30.seconds,
      factor: Double = 2.0,
      maxRetries: Option[Int] = None
  ): BackoffPolicy = new BackoffPolicy:
    private val random = new Random()

    override def delay(attempt: Int): Option[FiniteDuration] =
      maxRetries match
        case Some(maxAttempts) if attempt > maxAttempts => None
        case _ =>
          val exponentialDelay = base.toMillis.toDouble * math.pow(factor, (attempt - 1).toDouble)
          val cappedDelay = math.min(exponentialDelay, max.toMillis.toDouble)
          val jitteredDelay = (cappedDelay * random.nextDouble()).toLong
          Some(jitteredDelay.millis)

  /**
   * Create an exponential backoff policy with decorrelated jitter.
   *
   * Decorrelated jitter provides slightly better properties for load distribution
   * than full jitter by making each delay partially dependent on the previous.
   *
   * @param base The initial delay before first retry
   * @param max The maximum delay between retries
   * @param maxRetries Optional maximum number of retry attempts
   * @return A BackoffPolicy implementing decorrelated jitter
   */
  def decorrelatedJitter(
      base: FiniteDuration = 100.millis,
      max: FiniteDuration = 30.seconds,
      maxRetries: Option[Int] = None
  ): BackoffPolicy = new BackoffPolicy:
    private val random = new Random()
    @volatile private var lastDelay: Long = base.toMillis

    override def delay(attempt: Int): Option[FiniteDuration] =
      maxRetries match
        case Some(maxAttempts) if attempt > maxAttempts => None
        case _ =>
          if attempt == 1 then
            lastDelay = base.toMillis
            Some(base)
          else
            val nextDelay = math.min(max.toMillis, random.between(base.toMillis, lastDelay * 3))
            lastDelay = nextDelay
            Some(nextDelay.millis)

  /**
   * Create a fixed delay backoff policy.
   *
   * @param delay The fixed delay between retries
   * @param maxRetries Optional maximum number of retry attempts
   * @return A BackoffPolicy with constant delay
   */
  def fixed(
      fixedDelay: FiniteDuration,
      maxRetries: Option[Int] = None
  ): BackoffPolicy = new BackoffPolicy:
    override def delay(attempt: Int): Option[FiniteDuration] =
      maxRetries match
        case Some(maxAttempts) if attempt > maxAttempts => None
        case _ => Some(fixedDelay)

  /**
   * Create a backoff policy with no delay.
   *
   * @param maxRetries Maximum number of retry attempts
   * @return A BackoffPolicy with zero delay
   */
  def immediate(maxRetries: Int): BackoffPolicy = new BackoffPolicy:
    override def delay(attempt: Int): Option[FiniteDuration] =
      if attempt <= maxRetries then Some(Duration.Zero)
      else None

  /**
   * Create a backoff policy from a BackoffConfig.
   *
   * @param config The backoff configuration
   * @return A BackoffPolicy based on the config
   */
  def fromConfig(config: BackoffConfig): BackoffPolicy =
    exponentialWithJitter(
      base = config.baseDelay,
      max = config.maxDelay,
      factor = config.factor,
      maxRetries = config.maxRetries
    )

/**
 * Retry utilities using backoff policies.
 */
object Retry:

  /**
   * Retry an effect using a backoff policy.
   *
   * @param policy The backoff policy to use
   * @param fa The effect to retry
   * @param shouldRetry Predicate to determine if error is retryable
   * @param onRetry Callback invoked before each retry with (attempt, delay, error)
   * @tparam F The effect type
   * @tparam A The result type
   * @return The effect wrapped with retry logic
   */
  def withBackoff[F[_]: Async, A](
      policy: BackoffPolicy,
      fa: F[A],
      shouldRetry: Throwable => Boolean = _ => true,
      onRetry: Option[(Int, FiniteDuration, Throwable) => F[Unit]] = None
  ): F[A] =
    val retryCallback = onRetry.getOrElse((_, _, _) => Async[F].unit)
    
    def loop(attempt: Int): F[A] =
      fa.handleErrorWith { error =>
        if !shouldRetry(error) then
          Async[F].raiseError(error)
        else
          policy.delay(attempt + 1) match
            case Some(delay) =>
              retryCallback(attempt, delay, error) *>
                Async[F].sleep(delay) *>
                loop(attempt + 1)
            case None =>
              Async[F].raiseError(error)
      }

    loop(1)
