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

package fs2.nats.protocol

import fs2.Chunk
import java.nio.charset.StandardCharsets

/** NATS message headers following the NATS/1.0 format. Headers are key-value
  * pairs where keys are case-insensitive for lookup but preserved for
  * serialization. Multiple values per key are supported.
  *
  * Format:
  * {{{
  * NATS/1.0\r\n
  * Header-Name: header-value\r\n
  * Another-Header: value\r\n
  * \r\n
  * }}}
  *
  * @param entries
  *   The header entries as a Vector of key-value pairs
  */
final case class Headers(entries: Vector[(String, String)]):

  /** Get the first value for a header key (case-insensitive).
    *
    * @param key
    *   The header key to look up
    * @return
    *   The first value if found
    */
  def get(key: String): Option[String] =
    entries.find(_._1.equalsIgnoreCase(key)).map(_._2)

  /** Get all values for a header key (case-insensitive).
    *
    * @param key
    *   The header key to look up
    * @return
    *   All values for the key
    */
  def getAll(key: String): Vector[String] =
    entries.filter(_._1.equalsIgnoreCase(key)).map(_._2)

  /** Check if a header key exists (case-insensitive).
    *
    * @param key
    *   The header key to check
    * @return
    *   True if the key exists
    */
  def contains(key: String): Boolean =
    entries.exists(_._1.equalsIgnoreCase(key))

  /** Add a header entry. Does not replace existing entries with the same key.
    *
    * @param key
    *   The header key
    * @param value
    *   The header value
    * @return
    *   New Headers with the added entry
    */
  def add(key: String, value: String): Headers =
    Headers(entries :+ (key, value))

  /** Set a header value, replacing any existing entries with the same key.
    *
    * @param key
    *   The header key
    * @param value
    *   The header value
    * @return
    *   New Headers with the key set to the single value
    */
  def set(key: String, value: String): Headers =
    Headers(entries.filterNot(_._1.equalsIgnoreCase(key)) :+ (key, value))

  /** Remove all entries for a header key (case-insensitive).
    *
    * @param key
    *   The header key to remove
    * @return
    *   New Headers without entries for the key
    */
  def remove(key: String): Headers =
    Headers(entries.filterNot(_._1.equalsIgnoreCase(key)))

  /** Check if there are no headers.
    *
    * @return
    *   True if empty
    */
  def isEmpty: Boolean = entries.isEmpty

  /** Check if there are headers.
    *
    * @return
    *   True if non-empty
    */
  def nonEmpty: Boolean = entries.nonEmpty

  /** Get the number of header entries.
    *
    * @return
    *   The count of entries
    */
  def size: Int = entries.size

  /** Serialize headers to NATS/1.0 format bytes.
    *
    * @return
    *   Chunk of bytes in NATS header format
    */
  def toBytes: Chunk[Byte] =
    if entries.isEmpty then Chunk.empty
    else
      val sb = new StringBuilder
      sb.append(Headers.Version)
      sb.append(Headers.CRLF)
      entries.foreach { case (k, v) =>
        sb.append(k)
        sb.append(": ")
        sb.append(v)
        sb.append(Headers.CRLF)
      }
      sb.append(Headers.CRLF)
      Chunk.array(sb.toString.getBytes(StandardCharsets.UTF_8))

  /** Get the serialized byte length of these headers.
    *
    * @return
    *   The byte length when serialized
    */
  def byteLength: Int =
    if entries.isEmpty then 0
    else toBytes.size

object Headers:
  /** NATS header version string */
  val Version: String = "NATS/1.0"

  /** CRLF line terminator */
  val CRLF: String = "\r\n"

  /** Empty headers instance */
  val empty: Headers = Headers(Vector.empty)

  /** Create Headers from varargs of key-value pairs.
    *
    * @param pairs
    *   The key-value pairs
    * @return
    *   Headers containing the pairs
    */
  def apply(pairs: (String, String)*): Headers =
    Headers(pairs.toVector)

  /** Create Headers from a Map.
    *
    * @param map
    *   The map of header keys to values
    * @return
    *   Headers containing the map entries
    */
  def fromMap(map: Map[String, String]): Headers =
    Headers(map.toVector)

  /** Parse headers from NATS/1.0 format bytes.
    *
    * @param bytes
    *   The header bytes to parse
    * @return
    *   Either a parse error or the parsed Headers
    */
  def parse(bytes: Chunk[Byte]): Either[String, Headers] =
    val str = new String(bytes.toArray, StandardCharsets.UTF_8)
    parse(str)

  /** Parse headers from NATS/1.0 format string.
    *
    * @param str
    *   The header string to parse
    * @return
    *   Either a parse error or the parsed Headers
    */
  def parse(str: String): Either[String, Headers] =
    val lines = str.split("\r\n", -1).toVector
    if lines.isEmpty || !lines.head.startsWith(Version) then
      Left(s"Invalid NATS headers: missing or invalid version line")
    else
      val headerLines = lines.tail.takeWhile(_.nonEmpty)
      val parsed = headerLines.map { line =>
        val colonIdx = line.indexOf(':')
        if colonIdx < 0 then Left(s"Invalid header line (no colon): $line")
        else
          val key = line.substring(0, colonIdx).trim
          val value = line.substring(colonIdx + 1).trim
          Right((key, value))
      }

      val errors = parsed.collect { case Left(e) => e }
      if errors.nonEmpty then Left(errors.mkString("; "))
      else Right(Headers(parsed.collect { case Right(kv) => kv }))

  /** Parse headers from NATS/1.0 format bytes, extracting status code if
    * present. Status codes appear as "NATS/1.0 503" for no-responders, etc.
    *
    * @param bytes
    *   The header bytes to parse
    * @return
    *   Either a parse error or tuple of (optional status code, Headers)
    */
  def parseWithStatus(
      bytes: Chunk[Byte]
  ): Either[String, (Option[Int], Headers)] =
    val str = new String(bytes.toArray, StandardCharsets.UTF_8)
    parseWithStatus(str)

  /** Parse headers from NATS/1.0 format string, extracting status code if
    * present.
    *
    * @param str
    *   The header string to parse
    * @return
    *   Either a parse error or tuple of (optional status code, Headers)
    */
  def parseWithStatus(str: String): Either[String, (Option[Int], Headers)] =
    val lines = str.split("\r\n", -1).toVector
    if lines.isEmpty then Left("Invalid NATS headers: empty input")
    else
      val versionLine = lines.head
      if !versionLine.startsWith(Version) then
        Left(s"Invalid NATS headers: missing or invalid version line")
      else
        val statusCode =
          if versionLine.length > Version.length then
            val rest = versionLine.substring(Version.length).trim
            if rest.nonEmpty then rest.toIntOption else None
          else None

        val headerLines = lines.tail.takeWhile(_.nonEmpty)
        val parsed = headerLines.map { line =>
          val colonIdx = line.indexOf(':')
          if colonIdx < 0 then Left(s"Invalid header line (no colon): $line")
          else
            val key = line.substring(0, colonIdx).trim
            val value = line.substring(colonIdx + 1).trim
            Right((key, value))
        }

        val errors = parsed.collect { case Left(e) => e }
        if errors.nonEmpty then Left(errors.mkString("; "))
        else
          Right((statusCode, Headers(parsed.collect { case Right(kv) => kv })))
