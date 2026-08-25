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

/** Frozen, verbatim copies of the previous implementations of `Headers`, in
  * both directions: the `split("\r\n", -1)` parser as it stood before the
  * index-scan rewrite, and the `StringBuilder` + `getBytes(UTF_8)` serializer
  * as it stood before the write-into-a-sized-array rewrite. Used ONLY as the
  * A/B correctness oracle for [[HeadersPropSpec]].
  *
  * This is needed because [[ReferenceProtocolParser]] delegates to the *live*
  * `Headers.parseWithStatus`: rewriting `Headers` moves both sides of the
  * `ProtocolParserPropSpec` gate together, so that gate proves nothing about
  * header semantics. This file is the replacement gate.
  */
object ReferenceHeaders:

  private val Version: String = "NATS/1.0"

  /** Frozen, verbatim copy of the `StringBuilder` + `getBytes(UTF_8)`
    * implementation of `Headers.toBytes`.
    */
  def toBytes(entries: Vector[(String, String)]): Chunk[Byte] =
    if entries.isEmpty then Chunk.empty
    else
      val sb = new StringBuilder
      sb.append(Version)
      sb.append("\r\n")
      entries.foreach { case (k, v) =>
        sb.append(k)
        sb.append(": ")
        sb.append(v)
        sb.append("\r\n")
      }
      sb.append("\r\n")
      Chunk.array(sb.toString.getBytes(StandardCharsets.UTF_8))

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

  def parseWithStatus(
      str: String
  ): Either[String, (Option[Int], Option[String], Headers)] =
    val lines = str.split("\r\n", -1).toVector
    if lines.isEmpty then Left("Invalid NATS headers: empty input")
    else
      val versionLine = lines.head
      if !versionLine.startsWith(Version) then
        Left(s"Invalid NATS headers: missing or invalid version line")
      else
        // The version line is "NATS/1.0 <code> <description>" for control
        // messages (e.g. "NATS/1.0 100 Idle Heartbeat"). Split the leading
        // status code from the trailing description so callers can match on
        // both; a bare "NATS/1.0 503" yields a code with no description.
        val (statusCode, statusDescription) =
          if versionLine.length > Version.length then
            val rest = versionLine.substring(Version.length).trim
            if rest.isEmpty then (None, None)
            else
              val spaceIdx = rest.indexWhere(_.isWhitespace)
              if spaceIdx < 0 then (rest.toIntOption, None)
              else
                val code = rest.substring(0, spaceIdx).toIntOption
                val desc = rest.substring(spaceIdx + 1).trim
                (code, if desc.nonEmpty then Some(desc) else None)
          else (None, None)

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
          Right(
            (
              statusCode,
              statusDescription,
              Headers(parsed.collect { case Right(kv) => kv })
            )
          )
