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

import fs2.Chunk
import java.nio.charset.StandardCharsets

/** Utilities for serializing NATS protocol messages.
  *
  * All methods produce properly CRLF-terminated byte chunks following the NATS
  * protocol specification.
  */
object SerializationUtils:

  private val CRLF: Array[Byte] = "\r\n".getBytes(StandardCharsets.US_ASCII)

  private val PubPrefix: Array[Byte] = "PUB ".getBytes(StandardCharsets.US_ASCII)
  private val HpubPrefix: Array[Byte] =
    "HPUB ".getBytes(StandardCharsets.US_ASCII)
  private val Space: Byte = ' '.toByte
  private val Cr: Byte = '\r'.toByte
  private val Lf: Byte = '\n'.toByte

  // Static control frames never change, so build them once and share the
  // immutable Chunk. PONG in particular is sent on every server PING.
  private val PingChunk: Chunk[Byte] =
    Chunk.array("PING\r\n".getBytes(StandardCharsets.US_ASCII))
  private val PongChunk: Chunk[Byte] =
    Chunk.array("PONG\r\n".getBytes(StandardCharsets.US_ASCII))

  /** Convert a string to UTF-8 bytes.
    *
    * @param s
    *   The string to convert
    * @return
    *   UTF-8 encoded bytes
    */
  def toBytes(s: String): Array[Byte] =
    s.getBytes(StandardCharsets.UTF_8)

  /** Create a CRLF-terminated chunk from a string.
    *
    * @param s
    *   The string to terminate
    * @return
    *   Chunk with CRLF appended
    */
  def withCrlf(s: String): Chunk[Byte] =
    Chunk.array(toBytes(s) ++ CRLF)

  /** Create a CRLF chunk.
    *
    * @return
    *   Chunk containing only CRLF
    */
  def crlf: Chunk[Byte] =
    Chunk.array(CRLF)

  /** Build a PUB command. Format: PUB <subject> [reply-to]
    * <#bytes>\r\n[payload]\r\n
    *
    * @param subject
    *   The subject to publish to
    * @param replyTo
    *   Optional reply-to subject
    * @param payload
    *   The message payload
    * @return
    *   Complete PUB command as bytes
    */
  def buildPub(
      subject: String,
      replyTo: Option[String],
      payload: Chunk[Byte]
  ): Chunk[Byte] =
    val subjectBytes = subject.getBytes(StandardCharsets.UTF_8)
    val payloadSize = payload.size
    val replyBytes = replyTo match
      case Some(reply) => reply.getBytes(StandardCharsets.UTF_8)
      case None        => null
    val replyExtra = if replyBytes eq null then 0 else replyBytes.length + 1

    // PUB <subject> [reply-to ]<#bytes>\r\n[payload]\r\n
    val controlLen =
      PubPrefix.length + subjectBytes.length + 1 + replyExtra +
        numDigits(payloadSize) + 2
    val arr = new Array[Byte](controlLen + payloadSize + 2)

    System.arraycopy(PubPrefix, 0, arr, 0, PubPrefix.length)
    var off = PubPrefix.length
    System.arraycopy(subjectBytes, 0, arr, off, subjectBytes.length)
    off += subjectBytes.length
    arr(off) = Space
    off += 1
    if replyBytes ne null then
      System.arraycopy(replyBytes, 0, arr, off, replyBytes.length)
      off += replyBytes.length
      arr(off) = Space
      off += 1
    off = writeAsciiInt(arr, off, payloadSize)
    arr(off) = Cr
    arr(off + 1) = Lf
    off += 2
    payload.copyToArray(arr, off)
    off += payloadSize
    arr(off) = Cr
    arr(off + 1) = Lf

    Chunk.array(arr)

  /** Build an HPUB command. Format: HPUB <subject> [reply-to] <#header bytes>
    * <#total bytes>\r\n[headers]\r\n\r\n[payload]\r\n
    *
    * @param subject
    *   The subject to publish to
    * @param replyTo
    *   Optional reply-to subject
    * @param headers
    *   The serialized headers bytes (including NATS/1.0\r\n and terminating
    *   \r\n)
    * @param payload
    *   The message payload
    * @return
    *   Complete HPUB command as bytes
    */
  def buildHPub(
      subject: String,
      replyTo: Option[String],
      headers: Chunk[Byte],
      payload: Chunk[Byte]
  ): Chunk[Byte] =
    val subjectBytes = subject.getBytes(StandardCharsets.UTF_8)
    val headerBytes = headers.size
    val payloadSize = payload.size
    val totalBytes = headerBytes + payloadSize
    val replyBytes = replyTo match
      case Some(reply) => reply.getBytes(StandardCharsets.UTF_8)
      case None        => null
    val replyExtra = if replyBytes eq null then 0 else replyBytes.length + 1

    // HPUB <subject> [reply-to ]<#header bytes> <#total bytes>\r\n[headers][payload]\r\n
    val controlLen =
      HpubPrefix.length + subjectBytes.length + 1 + replyExtra +
        numDigits(headerBytes) + 1 + numDigits(totalBytes) + 2
    val arr = new Array[Byte](controlLen + totalBytes + 2)

    System.arraycopy(HpubPrefix, 0, arr, 0, HpubPrefix.length)
    var off = HpubPrefix.length
    System.arraycopy(subjectBytes, 0, arr, off, subjectBytes.length)
    off += subjectBytes.length
    arr(off) = Space
    off += 1
    if replyBytes ne null then
      System.arraycopy(replyBytes, 0, arr, off, replyBytes.length)
      off += replyBytes.length
      arr(off) = Space
      off += 1
    off = writeAsciiInt(arr, off, headerBytes)
    arr(off) = Space
    off += 1
    off = writeAsciiInt(arr, off, totalBytes)
    arr(off) = Cr
    arr(off + 1) = Lf
    off += 2
    headers.copyToArray(arr, off)
    off += headerBytes
    payload.copyToArray(arr, off)
    off += payloadSize
    arr(off) = Cr
    arr(off + 1) = Lf

    Chunk.array(arr)

  /** Number of ASCII decimal digits needed to render a non-negative Int. */
  private def numDigits(n: Int): Int =
    if n < 10 then 1
    else if n < 100 then 2
    else if n < 1000 then 3
    else if n < 10000 then 4
    else if n < 100000 then 5
    else if n < 1000000 then 6
    else if n < 10000000 then 7
    else if n < 100000000 then 8
    else if n < 1000000000 then 9
    else 10

  /** Write a non-negative Int as ASCII decimal digits into `arr` starting at
    * `offset`, returning the offset just past the last digit written.
    */
  private def writeAsciiInt(arr: Array[Byte], offset: Int, value: Int): Int =
    if value == 0 then
      arr(offset) = '0'.toByte
      offset + 1
    else
      val stop = offset + numDigits(value)
      var i = stop - 1
      var v = value
      while v > 0 do
        arr(i) = ('0' + (v % 10)).toByte
        v /= 10
        i -= 1
      stop

  /** Build a SUB command. Format: SUB <subject> [queue group] <sid>\r\n
    *
    * @param subject
    *   The subject to subscribe to
    * @param queueGroup
    *   Optional queue group
    * @param sid
    *   The subscription ID
    * @return
    *   Complete SUB command as bytes
    */
  def buildSub(
      subject: String,
      queueGroup: Option[String],
      sid: Long
  ): Chunk[Byte] =
    val controlLine = queueGroup match
      case Some(group) =>
        s"SUB $subject $group $sid"
      case None =>
        s"SUB $subject $sid"

    withCrlf(controlLine)

  /** Build an UNSUB command. Format: UNSUB <sid> [max_msgs]\r\n
    *
    * @param sid
    *   The subscription ID
    * @param maxMsgs
    *   Optional max messages before auto-unsubscribe
    * @return
    *   Complete UNSUB command as bytes
    */
  def buildUnsub(sid: Long, maxMsgs: Option[Int]): Chunk[Byte] =
    val controlLine = maxMsgs match
      case Some(max) =>
        s"UNSUB $sid $max"
      case None =>
        s"UNSUB $sid"

    withCrlf(controlLine)

  /** Build a CONNECT command. Format: CONNECT <json>\r\n
    *
    * @param json
    *   The JSON payload for CONNECT
    * @return
    *   Complete CONNECT command as bytes
    */
  def buildConnect(json: String): Chunk[Byte] =
    withCrlf(s"CONNECT $json")

  /** Build a PING command.
    *
    * @return
    *   Complete PING command as bytes
    */
  def buildPing: Chunk[Byte] = PingChunk

  /** Build a PONG command.
    *
    * @return
    *   Complete PONG command as bytes
    */
  def buildPong: Chunk[Byte] = PongChunk

  /** Calculate the byte length of a string when encoded as UTF-8.
    *
    * @param s
    *   The string to measure
    * @return
    *   The byte length
    */
  def byteLength(s: String): Int =
    s.getBytes(StandardCharsets.UTF_8).length

  /** Validate a NATS subject. Subjects cannot be empty, contain spaces, or
    * start/end with '.'.
    *
    * @param subject
    *   The subject to validate
    * @return
    *   Either an error message or the valid subject
    */
  def validateSubject(subject: String): Either[String, String] =
    if subject.isEmpty then Left("Subject cannot be empty")
    else if subject.contains(' ') then Left("Subject cannot contain spaces")
    else if subject.contains('\t') then Left("Subject cannot contain tabs")
    else if subject.startsWith(".") || subject.endsWith(".") then
      Left("Subject cannot start or end with '.'")
    else if subject.contains("..") then Left("Subject cannot contain '..'")
    else Right(subject)
