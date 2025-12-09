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

import fs2.Chunk
import fs2.nats.protocol.Headers

/**
 * Represents a message received from a NATS subscription.
 * This is the user-facing message type delivered through subscription streams.
 *
 * @param subject The subject the message was published to
 * @param replyTo Optional reply-to subject for request/reply pattern
 * @param headers The message headers (empty if no headers)
 * @param payload The message payload as raw bytes
 * @param sid The subscription ID that received this message (internal use)
 */
final case class NatsMessage(
    subject: String,
    replyTo: Option[String],
    headers: Headers,
    payload: Chunk[Byte],
    sid: Long
):

  /**
   * Get the payload as a UTF-8 string.
   *
   * @return The payload decoded as UTF-8
   */
  def payloadAsString: String =
    new String(payload.toArray, java.nio.charset.StandardCharsets.UTF_8)

  /**
   * Get the payload size in bytes.
   *
   * @return The size of the payload
   */
  def payloadSize: Int = payload.size

  /**
   * Check if this message has a reply-to subject.
   *
   * @return True if replyTo is defined
   */
  def isRequest: Boolean = replyTo.isDefined

  /**
   * Check if this message has headers.
   *
   * @return True if headers are present
   */
  def hasHeaders: Boolean = headers.nonEmpty

object NatsMessage:
  /**
   * Create a message with just a subject and payload (no headers).
   *
   * @param subject The message subject
   * @param payload The message payload
   * @param sid The subscription ID
   * @return A NatsMessage without headers or reply-to
   */
  def simple(subject: String, payload: Chunk[Byte], sid: Long): NatsMessage =
    NatsMessage(
      subject = subject,
      replyTo = None,
      headers = Headers.empty,
      payload = payload,
      sid = sid
    )

  /**
   * Create a message from a UTF-8 string payload.
   *
   * @param subject The message subject
   * @param payload The string payload
   * @param sid The subscription ID
   * @return A NatsMessage with the string encoded as UTF-8
   */
  def fromString(subject: String, payload: String, sid: Long): NatsMessage =
    NatsMessage(
      subject = subject,
      replyTo = None,
      headers = Headers.empty,
      payload = Chunk.array(payload.getBytes(java.nio.charset.StandardCharsets.UTF_8)),
      sid = sid
    )
