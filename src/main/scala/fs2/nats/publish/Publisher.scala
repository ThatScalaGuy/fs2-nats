/*
 * Copyright 2024 fs2-nats contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package fs2.nats.publish

import cats.effect.Async
import cats.syntax.all.*
import fs2.Chunk
import fs2.nats.errors.NatsError
import fs2.nats.protocol.Headers
import fs2.nats.transport.Transport

/**
 * Publisher for sending messages to NATS subjects.
 *
 * Responsible for:
 * - Building PUB/HPUB byte frames
 * - Computing byte lengths for protocol fields
 * - Enforcing max_payload limits
 * - Enqueueing messages to the transport's outbound queue
 *
 * @tparam F The effect type
 */
trait Publisher[F[_]]:

  /**
   * Publish a message without headers.
   *
   * @param subject The subject to publish to
   * @param payload The message payload
   * @param replyTo Optional reply-to subject for request/reply
   * @return Effect that completes when message is enqueued
   */
  def publish(
      subject: String,
      payload: Chunk[Byte],
      replyTo: Option[String] = None
  ): F[Unit]

  /**
   * Publish a message with headers.
   *
   * @param subject The subject to publish to
   * @param payload The message payload
   * @param headers The message headers
   * @param replyTo Optional reply-to subject for request/reply
   * @return Effect that completes when message is enqueued
   */
  def publishWithHeaders(
      subject: String,
      payload: Chunk[Byte],
      headers: Headers,
      replyTo: Option[String] = None
  ): F[Unit]

  /**
   * Update the max_payload limit from server INFO.
   *
   * @param maxPayload The new maximum payload size in bytes
   * @return Effect that completes when limit is updated
   */
  def updateMaxPayload(maxPayload: Long): F[Unit]

object Publisher:

  /**
   * Create a new Publisher.
   *
   * @param transport The transport to send messages through
   * @param initialMaxPayload Initial max_payload limit (updated when server INFO received)
   * @tparam F The effect type
   * @return A new Publisher instance
   */
  def apply[F[_]: Async](
      transport: Transport[F],
      initialMaxPayload: Long = 1024 * 1024
  ): F[Publisher[F]] =
    cats.effect.Ref.of[F, Long](initialMaxPayload).map { maxPayloadRef =>
      new PublisherImpl[F](transport, maxPayloadRef)
    }

  private class PublisherImpl[F[_]: Async](
      transport: Transport[F],
      maxPayloadRef: cats.effect.Ref[F, Long]
  ) extends Publisher[F]:

    override def publish(
        subject: String,
        payload: Chunk[Byte],
        replyTo: Option[String]
    ): F[Unit] =
      for
        _ <- validateSubject(subject)
        maxPayload <- maxPayloadRef.get
        _ <- validatePayloadSize(payload.size.toLong, maxPayload)
        bytes = SerializationUtils.buildPub(subject, replyTo, payload)
        _ <- transport.send(bytes)
      yield ()

    override def publishWithHeaders(
        subject: String,
        payload: Chunk[Byte],
        headers: Headers,
        replyTo: Option[String]
    ): F[Unit] =
      for
        _ <- validateSubject(subject)
        headerBytes = headers.toBytes
        totalSize = headerBytes.size.toLong + payload.size.toLong
        maxPayload <- maxPayloadRef.get
        _ <- validatePayloadSize(totalSize, maxPayload)
        bytes = SerializationUtils.buildHPub(subject, replyTo, headerBytes, payload)
        _ <- transport.send(bytes)
      yield ()

    override def updateMaxPayload(maxPayload: Long): F[Unit] =
      maxPayloadRef.set(maxPayload)

    private def validateSubject(subject: String): F[Unit] =
      SerializationUtils.validateSubject(subject) match
        case Right(_) => Async[F].unit
        case Left(reason) =>
          Async[F].raiseError(NatsError.InvalidSubject(subject, reason))

    private def validatePayloadSize(size: Long, maxPayload: Long): F[Unit] =
      if size > maxPayload then
        Async[F].raiseError(NatsError.PayloadTooLarge(size, maxPayload))
      else
        Async[F].unit
