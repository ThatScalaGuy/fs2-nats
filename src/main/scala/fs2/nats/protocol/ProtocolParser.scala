/*
 * Copyright 2024 fs2-nats contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package fs2.nats.protocol

import cats.effect.Async
import cats.syntax.all.*
import fs2.{Chunk, Pipe, Pull, Stream}
import java.nio.charset.StandardCharsets

/**
 * Configuration for the NATS protocol parser.
 *
 * @param maxControlLineLength Maximum allowed length for control lines (default 4096)
 * @param maxPayloadLimit Optional safety cap on payload size (None = no limit)
 * @param strictCRLF Whether to require strict CRLF line endings (default true)
 * @param strictMode Whether to fail on incomplete final frames (true) or emit error frames (false)
 */
final case class ParserConfig(
    maxControlLineLength: Int = 4096,
    maxPayloadLimit: Option[Long] = None,
    strictCRLF: Boolean = true,
    strictMode: Boolean = true
)

object ParserConfig:
  val default: ParserConfig = ParserConfig()

/**
 * Incremental NATS protocol parser.
 *
 * Converts a raw Stream[F, Byte] into a Stream[F, NatsFrame] following the NATS protocol specification.
 * Handles partial reads, CRLF framing, and exact N-byte payload reads for MSG/HMSG/PUB/HPUB commands.
 */
object ProtocolParser:

  private val CR: Byte = '\r'.toByte
  private val LF: Byte = '\n'.toByte
  private val SPACE: Byte = ' '.toByte
  private val TAB: Byte = '\t'.toByte

  /**
   * Create a parsing pipe that transforms bytes into NATS frames.
   *
   * @tparam F The effect type
   * @return A Pipe that parses bytes into NatsFrame values
   */
  def parseStream[F[_]: Async]: Pipe[F, Byte, NatsFrame] =
    parseStream(ParserConfig.default)

  /**
   * Create a parsing pipe with custom configuration.
   *
   * @param config Parser configuration
   * @tparam F The effect type
   * @return A Pipe that parses bytes into NatsFrame values
   */
  def parseStream[F[_]: Async](config: ParserConfig): Pipe[F, Byte, NatsFrame] =
    input => parseLoop(config, Chunk.empty, input).stream

  private def parseLoop[F[_]: Async](
      config: ParserConfig,
      buffer: Chunk[Byte],
      input: Stream[F, Byte]
  ): Pull[F, NatsFrame, Unit] =
    findCrlf(buffer) match
      case Some(crlfIdx) =>
        val controlLine = buffer.take(crlfIdx)
        val remaining = buffer.drop(crlfIdx + 2)
        
        if controlLine.size > config.maxControlLineLength then
          emitParseError(config, s"Control line exceeds maximum length of ${config.maxControlLineLength}")
        else
          parseControlLine(config, controlLine, remaining, input)

      case None =>
        if buffer.size > config.maxControlLineLength then
          emitParseError(config, s"Control line exceeds maximum length of ${config.maxControlLineLength}")
        else
          input.pull.uncons.flatMap {
            case Some((chunk, rest)) =>
              parseLoop(config, buffer ++ chunk, rest)
            case None =>
              if buffer.isEmpty then Pull.done
              else if config.strictMode then
                Pull.raiseError(
                  fs2.nats.errors.NatsError.ProtocolParseError("Incomplete control line at end of stream")
                )
              else
                Pull.output1(NatsFrame.ParseErrorFrame("Incomplete control line at end of stream")) >> Pull.done
          }

  private def parseControlLine[F[_]: Async](
      config: ParserConfig,
      controlLine: Chunk[Byte],
      remaining: Chunk[Byte],
      input: Stream[F, Byte]
  ): Pull[F, NatsFrame, Unit] =
    val line = new String(controlLine.toArray, StandardCharsets.UTF_8)
    val tokens = tokenize(line)

    tokens.headOption.map(_.toUpperCase) match
      case Some("INFO") =>
        parseInfoFrame(config, line, remaining, input)

      case Some("MSG") =>
        parseMsgFrame(config, tokens, remaining, input)

      case Some("HMSG") =>
        parseHMsgFrame(config, tokens, remaining, input)

      case Some("PING") =>
        Pull.output1(NatsFrame.PingFrame) >> parseLoop(config, remaining, input)

      case Some("PONG") =>
        Pull.output1(NatsFrame.PongFrame) >> parseLoop(config, remaining, input)

      case Some("+OK") =>
        Pull.output1(NatsFrame.OkFrame) >> parseLoop(config, remaining, input)

      case Some("-ERR") =>
        val msg = line.drop(4).trim.stripPrefix("'").stripSuffix("'")
        Pull.output1(NatsFrame.ErrFrame(msg)) >> parseLoop(config, remaining, input)

      case Some(cmd) =>
        emitParseError(config, s"Unrecognized command: $cmd")

      case None =>
        parseLoop(config, remaining, input)

  private def parseInfoFrame[F[_]: Async](
      config: ParserConfig,
      line: String,
      remaining: Chunk[Byte],
      input: Stream[F, Byte]
  ): Pull[F, NatsFrame, Unit] =
    val jsonStr = line.drop(5).trim
    io.circe.parser.decode[Info](jsonStr) match
      case Right(info) =>
        Pull.output1(NatsFrame.InfoFrame(info)) >> parseLoop(config, remaining, input)
      case Left(err) =>
        emitParseError(config, s"Failed to parse INFO JSON: ${err.getMessage}")

  private def parseMsgFrame[F[_]: Async](
      config: ParserConfig,
      tokens: Vector[String],
      remaining: Chunk[Byte],
      input: Stream[F, Byte]
  ): Pull[F, NatsFrame, Unit] =
    tokens match
      case Vector("MSG", subject, sidStr, replyTo, lengthStr) =>
        (parseLong(sidStr), parseInt(lengthStr)) match
          case (Some(sid), Some(length)) =>
            validatePayloadLength(config, length) match
              case Some(errPull) => errPull
              case None =>
                readPayload(config, remaining, input, length).flatMap { case (payload, afterPayload, stream) =>
                  Pull.output1(NatsFrame.MsgFrame(subject, sid, Some(replyTo), payload)) >>
                    parseLoop(config, afterPayload, stream)
                }
          case _ =>
            emitParseError(config, s"Invalid MSG frame: ${tokens.mkString(" ")}")

      case Vector("MSG", subject, sidStr, lengthStr) =>
        (parseLong(sidStr), parseInt(lengthStr)) match
          case (Some(sid), Some(length)) =>
            validatePayloadLength(config, length) match
              case Some(errPull) => errPull
              case None =>
                readPayload(config, remaining, input, length).flatMap { case (payload, afterPayload, stream) =>
                  Pull.output1(NatsFrame.MsgFrame(subject, sid, None, payload)) >>
                    parseLoop(config, afterPayload, stream)
                }
          case _ =>
            emitParseError(config, s"Invalid MSG frame: ${tokens.mkString(" ")}")

      case _ =>
        emitParseError(config, s"Invalid MSG frame format: ${tokens.mkString(" ")}")

  private def parseHMsgFrame[F[_]: Async](
      config: ParserConfig,
      tokens: Vector[String],
      remaining: Chunk[Byte],
      input: Stream[F, Byte]
  ): Pull[F, NatsFrame, Unit] =
    tokens match
      case Vector("HMSG", subject, sidStr, replyTo, headerLenStr, totalLenStr) =>
        (parseLong(sidStr), parseInt(headerLenStr), parseInt(totalLenStr)) match
          case (Some(sid), Some(headerLen), Some(totalLen)) =>
            validatePayloadLength(config, totalLen.toLong) match
              case Some(errPull) => errPull
              case None =>
                readHMsgPayload(config, remaining, input, subject, sid, Some(replyTo), headerLen, totalLen)
          case _ =>
            emitParseError(config, s"Invalid HMSG frame: ${tokens.mkString(" ")}")

      case Vector("HMSG", subject, sidStr, headerLenStr, totalLenStr) =>
        (parseLong(sidStr), parseInt(headerLenStr), parseInt(totalLenStr)) match
          case (Some(sid), Some(headerLen), Some(totalLen)) =>
            validatePayloadLength(config, totalLen.toLong) match
              case Some(errPull) => errPull
              case None =>
                readHMsgPayload(config, remaining, input, subject, sid, None, headerLen, totalLen)
          case _ =>
            emitParseError(config, s"Invalid HMSG frame: ${tokens.mkString(" ")}")

      case _ =>
        emitParseError(config, s"Invalid HMSG frame format: ${tokens.mkString(" ")}")

  private def readHMsgPayload[F[_]: Async](
      config: ParserConfig,
      remaining: Chunk[Byte],
      input: Stream[F, Byte],
      subject: String,
      sid: Long,
      replyTo: Option[String],
      headerLen: Int,
      totalLen: Int
  ): Pull[F, NatsFrame, Unit] =
    readPayload(config, remaining, input, totalLen).flatMap { case (fullPayload, afterPayload, stream) =>
      val headerBytes = fullPayload.take(headerLen)
      val payloadBytes = fullPayload.drop(headerLen)
      
      Headers.parseWithStatus(headerBytes) match
        case Right((statusCode, headers)) =>
          Pull.output1(NatsFrame.HMsgFrame(subject, sid, replyTo, headers, statusCode, payloadBytes)) >>
            parseLoop(config, afterPayload, stream)
        case Left(err) =>
          emitParseError(config, s"Failed to parse HMSG headers: $err")
    }

  private def readPayload[F[_]: Async](
      config: ParserConfig,
      buffer: Chunk[Byte],
      input: Stream[F, Byte],
      length: Int
  ): Pull[F, Nothing, (Chunk[Byte], Chunk[Byte], Stream[F, Byte])] =
    val needed = length + 2

    def accumulate(buf: Chunk[Byte], stream: Stream[F, Byte]): Pull[F, Nothing, (Chunk[Byte], Chunk[Byte], Stream[F, Byte])] =
      if buf.size >= needed then
        val payload = buf.take(length)
        val afterPayload = buf.drop(length)
        
        if afterPayload.size >= 2 then
          val crlf = afterPayload.take(2)
          if crlf(0) == CR && crlf(1) == LF then
            Pull.pure((payload, afterPayload.drop(2), stream))
          else if config.strictCRLF then
            Pull.raiseError(
              fs2.nats.errors.NatsError.ProtocolParseError(
                s"Missing CRLF after payload (got 0x${String.format("%02x", crlf(0))} 0x${String.format("%02x", crlf(1))})"
              )
            )
          else
            Pull.pure((payload, afterPayload.drop(2), stream))
        else
          stream.pull.uncons.flatMap {
            case Some((chunk, rest)) =>
              accumulate(buf ++ chunk, rest)
            case None =>
              Pull.raiseError(
                fs2.nats.errors.NatsError.ProtocolParseError("Unexpected end of stream while reading payload CRLF")
              )
          }
      else
        stream.pull.uncons.flatMap {
          case Some((chunk, rest)) =>
            accumulate(buf ++ chunk, rest)
          case None =>
            Pull.raiseError(
              fs2.nats.errors.NatsError.ProtocolParseError(s"Unexpected end of stream while reading payload (expected $length bytes)")
            )
        }

    accumulate(buffer, input)

  private def validatePayloadLength[F[_]: Async](config: ParserConfig, length: Long): Option[Pull[F, NatsFrame, Unit]] =
    config.maxPayloadLimit match
      case Some(max) if length > max =>
        Some(emitParseError(config, s"Payload size $length exceeds maximum of $max"))
      case _ if length < 0 =>
        Some(emitParseError(config, s"Invalid negative payload length: $length"))
      case _ =>
        None

  private def emitParseError[F[_]: Async](config: ParserConfig, message: String): Pull[F, NatsFrame, Unit] =
    if config.strictMode then
      Pull.raiseError(fs2.nats.errors.NatsError.ProtocolParseError(message))
    else
      Pull.output1(NatsFrame.ParseErrorFrame(message)) >> Pull.done

  private def findCrlf(chunk: Chunk[Byte]): Option[Int] =
    val arr = chunk.toArray
    var i = 0
    while i < arr.length - 1 do
      if arr(i) == CR && arr(i + 1) == LF then
        return Some(i)
      i += 1
    None

  private def tokenize(line: String): Vector[String] =
    line.split("\\s+").toVector.filter(_.nonEmpty)

  private def parseInt(s: String): Option[Int] =
    try Some(s.toInt)
    catch case _: NumberFormatException => None

  private def parseLong(s: String): Option[Long] =
    try Some(s.toLong)
    catch case _: NumberFormatException => None
