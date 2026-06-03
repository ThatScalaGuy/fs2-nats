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

package fs2.nats.publish

import cats.effect.Async
import fs2.Chunk
import fs2.nats.errors.NatsError
import fs2.nats.protocol.Headers
import fs2.nats.transport.Transport

/** Publisher for sending messages to NATS subjects.
  *
  * Responsible for:
  *   - Building PUB/HPUB byte frames
  *   - Computing byte lengths for protocol fields
  *   - Enforcing max_payload limits
  *   - Enqueueing messages to the transport's outbound queue
  *
  * @tparam F
  *   The effect type
  */
trait Publisher[F[_]]:

  /** Publish a message without headers.
    *
    * @param subject
    *   The subject to publish to
    * @param payload
    *   The message payload
    * @param replyTo
    *   Optional reply-to subject for request/reply
    * @return
    *   Effect that completes when message is enqueued
    */
  def publish(
      subject: String,
      payload: Chunk[Byte],
      replyTo: Option[String] = None
  ): F[Unit]

  /** Publish a message with headers.
    *
    * @param subject
    *   The subject to publish to
    * @param payload
    *   The message payload
    * @param headers
    *   The message headers
    * @param replyTo
    *   Optional reply-to subject for request/reply
    * @return
    *   Effect that completes when message is enqueued
    */
  def publishWithHeaders(
      subject: String,
      payload: Chunk[Byte],
      headers: Headers,
      replyTo: Option[String] = None
  ): F[Unit]

  /** Update the max_payload limit from server INFO.
    *
    * @param maxPayload
    *   The new maximum payload size in bytes
    * @return
    *   Effect that completes when limit is updated
    */
  def updateMaxPayload(maxPayload: Long): F[Unit]

object Publisher:

  /** Create a new Publisher.
    *
    * @param transport
    *   The transport to send messages through
    * @param initialMaxPayload
    *   Initial max_payload limit (updated when server INFO received)
    * @tparam F
    *   The effect type
    * @return
    *   A new Publisher instance
    */
  def apply[F[_]: Async](
      transport: Transport[F],
      initialMaxPayload: Long = 1024 * 1024
  ): F[Publisher[F]] =
    Async[F].delay(new PublisherImpl[F](transport, initialMaxPayload))

  private class PublisherImpl[F[_]: Async](
      transport: Transport[F],
      initialMaxPayload: Long
  ) extends Publisher[F]:

    // Read once per publish as a plain volatile field instead of a Ref effect;
    // refreshed when server INFO arrives.
    @volatile private var maxPayload: Long = initialMaxPayload

    override def publish(
        subject: String,
        payload: Chunk[Byte],
        replyTo: Option[String]
    ): F[Unit] =
      Async[F].defer {
        SerializationUtils.validateSubject(subject) match
          case Left(reason) =>
            Async[F].raiseError(NatsError.InvalidSubject(subject, reason))
          case Right(_) =>
            val size = payload.size.toLong
            if size > maxPayload then
              Async[F].raiseError(NatsError.PayloadTooLarge(size, maxPayload))
            else
              transport.send(
                SerializationUtils.buildPub(subject, replyTo, payload)
              )
      }

    override def publishWithHeaders(
        subject: String,
        payload: Chunk[Byte],
        headers: Headers,
        replyTo: Option[String]
    ): F[Unit] =
      Async[F].defer {
        SerializationUtils.validateSubject(subject) match
          case Left(reason) =>
            Async[F].raiseError(NatsError.InvalidSubject(subject, reason))
          case Right(_) =>
            val headerBytes = headers.toBytes
            val totalSize = headerBytes.size.toLong + payload.size.toLong
            if totalSize > maxPayload then
              Async[F].raiseError(
                NatsError.PayloadTooLarge(totalSize, maxPayload)
              )
            else
              transport.send(
                SerializationUtils
                  .buildHPub(subject, replyTo, headerBytes, payload)
              )
      }

    override def updateMaxPayload(maxPayload: Long): F[Unit] =
      Async[F].delay { this.maxPayload = maxPayload }
