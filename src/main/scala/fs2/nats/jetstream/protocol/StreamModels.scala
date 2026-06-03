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

package fs2.nats.jetstream.protocol

import fs2.Chunk
import fs2.nats.protocol.Headers
import io.circe.syntax.*
import io.circe.{Decoder, Encoder, Json}

import java.time.Instant
import scala.concurrent.duration.FiniteDuration

/** Stream configuration. Mirrors the JetStream `StreamConfig` wire model.
  *
  * Note: mirror/sources/placement/republish/subject-transform are not modeled
  * in this MVP; updating a stream that uses them will not preserve those
  * server-set fields.
  */
final case class StreamConfig(
    name: String,
    subjects: List[String] = Nil,
    retention: RetentionPolicy = RetentionPolicy.Limits,
    storage: StorageType = StorageType.File,
    maxConsumers: Int = -1,
    maxMsgs: Long = -1,
    maxBytes: Long = -1,
    maxAge: Option[FiniteDuration] = None,
    maxMsgSize: Int = -1,
    maxMsgsPerSubject: Long = -1,
    replicas: Int = 1,
    discard: DiscardPolicy = DiscardPolicy.Old,
    duplicateWindow: Option[FiniteDuration] = None,
    description: Option[String] = None,
    compression: StoreCompression = StoreCompression.None,
    allowDirect: Boolean = false
)

object StreamConfig:
  given Encoder[StreamConfig] = Encoder.instance { c =>
    JsWire.obj(
      Some("name" -> Json.fromString(c.name)),
      Some("subjects" -> Json.fromValues(c.subjects.map(Json.fromString))),
      Some("retention" -> c.retention.asJson),
      Some("storage" -> c.storage.asJson),
      Some("max_consumers" -> Json.fromInt(c.maxConsumers)),
      Some("max_msgs" -> Json.fromLong(c.maxMsgs)),
      Some("max_bytes" -> Json.fromLong(c.maxBytes)),
      c.maxAge.map(d => "max_age" -> JsWire.durationToNanos(d)),
      Some("max_msg_size" -> Json.fromInt(c.maxMsgSize)),
      Some("max_msgs_per_subject" -> Json.fromLong(c.maxMsgsPerSubject)),
      Some("num_replicas" -> Json.fromInt(c.replicas)),
      Some("discard" -> c.discard.asJson),
      c.duplicateWindow.map(d =>
        "duplicate_window" -> JsWire.durationToNanos(d)
      ),
      c.description.map(d => "description" -> Json.fromString(d)),
      Some("compression" -> c.compression.asJson),
      Some("allow_direct" -> Json.fromBoolean(c.allowDirect))
    )
  }

  given Decoder[StreamConfig] = Decoder.instance { c =>
    for
      name <- c.downField("name").as[String]
      subjects <- c
        .downField("subjects")
        .as[Option[List[String]]]
        .map(_.getOrElse(Nil))
      retention <- c
        .downField("retention")
        .as[Option[RetentionPolicy]]
        .map(_.getOrElse(RetentionPolicy.Limits))
      storage <- c
        .downField("storage")
        .as[Option[StorageType]]
        .map(_.getOrElse(StorageType.File))
      maxConsumers <- c
        .downField("max_consumers")
        .as[Option[Int]]
        .map(_.getOrElse(-1))
      maxMsgs <- c.downField("max_msgs").as[Option[Long]].map(_.getOrElse(-1L))
      maxBytes <- c
        .downField("max_bytes")
        .as[Option[Long]]
        .map(_.getOrElse(-1L))
      maxAge <- JsWire.optDurationNanos(c.downField("max_age"))
      maxMsgSize <- c
        .downField("max_msg_size")
        .as[Option[Int]]
        .map(_.getOrElse(-1))
      maxMsgsPerSubject <- c
        .downField("max_msgs_per_subject")
        .as[Option[Long]]
        .map(_.getOrElse(-1L))
      replicas <- c
        .downField("num_replicas")
        .as[Option[Int]]
        .map(_.getOrElse(1))
      discard <- c
        .downField("discard")
        .as[Option[DiscardPolicy]]
        .map(_.getOrElse(DiscardPolicy.Old))
      duplicateWindow <- JsWire.optDurationNanos(
        c.downField("duplicate_window")
      )
      description <- c.downField("description").as[Option[String]]
      compression <- c
        .downField("compression")
        .as[Option[StoreCompression]]
        .map(_.getOrElse(StoreCompression.None))
      allowDirect <- c
        .downField("allow_direct")
        .as[Option[Boolean]]
        .map(_.getOrElse(false))
    yield StreamConfig(
      name,
      subjects,
      retention,
      storage,
      maxConsumers,
      maxMsgs,
      maxBytes,
      maxAge,
      maxMsgSize,
      maxMsgsPerSubject,
      replicas,
      discard,
      duplicateWindow,
      description,
      compression,
      allowDirect
    )
  }

/** Runtime state of a stream. */
final case class StreamState(
    messages: Long,
    bytes: Long,
    firstSeq: Long,
    lastSeq: Long,
    consumerCount: Int
)

object StreamState:
  given Decoder[StreamState] = Decoder.instance { c =>
    for
      messages <- c.downField("messages").as[Option[Long]].map(_.getOrElse(0L))
      bytes <- c.downField("bytes").as[Option[Long]].map(_.getOrElse(0L))
      firstSeq <- c.downField("first_seq").as[Option[Long]].map(_.getOrElse(0L))
      lastSeq <- c.downField("last_seq").as[Option[Long]].map(_.getOrElse(0L))
      consumerCount <- c
        .downField("consumer_count")
        .as[Option[Int]]
        .map(_.getOrElse(0))
    yield StreamState(messages, bytes, firstSeq, lastSeq, consumerCount)
  }

/** Stream information returned by create/update/info. */
final case class StreamInfo(
    config: StreamConfig,
    state: StreamState,
    created: Instant
)

object StreamInfo:
  given Decoder[StreamInfo] =
    import JsWire.given
    Decoder.instance { c =>
      for
        config <- c.downField("config").as[StreamConfig]
        state <- c.downField("state").as[StreamState]
        created <- c.downField("created").as[Instant]
      yield StreamInfo(config, state, created)
    }

/** Options controlling a stream purge. */
final case class PurgeOptions(
    filter: Option[String] = None,
    seq: Option[Long] = None,
    keep: Option[Long] = None
)
object PurgeOptions:
  val all: PurgeOptions = PurgeOptions()

/** Result of a stream purge. */
final case class PurgeResponse(success: Boolean, purged: Long)
object PurgeResponse:
  given Decoder[PurgeResponse] = Decoder.instance { c =>
    for
      success <- c
        .downField("success")
        .as[Option[Boolean]]
        .map(_.getOrElse(false))
      purged <- c.downField("purged").as[Option[Long]].map(_.getOrElse(0L))
    yield PurgeResponse(success, purged)
  }

/** Query selecting a stored message to fetch. */
sealed trait MessageGet
object MessageGet:
  final case class BySeq(seq: Long) extends MessageGet
  final case class LastBySubject(subject: String) extends MessageGet

/** A message read back from a stream via `getMessage`. */
final case class StoredMessage(
    subject: String,
    seq: Long,
    headers: Headers,
    data: Chunk[Byte],
    time: Instant
)
object StoredMessage:
  given Decoder[StoredMessage] =
    import JsWire.given
    Decoder.instance { c =>
      for
        subject <- c.downField("subject").as[String]
        seq <- c.downField("seq").as[Long]
        hdrs <- c.downField("hdrs").as[Option[Chunk[Byte]]]
        headers <- hdrs match
          case None        => Right(Headers.empty)
          case Some(bytes) =>
            Headers
              .parse(
                new String(
                  bytes.toArray,
                  java.nio.charset.StandardCharsets.UTF_8
                )
              )
              .left
              .map(e => io.circe.DecodingFailure(e, c.history))
        data <- c
          .downField("data")
          .as[Option[Chunk[Byte]]]
          .map(_.getOrElse(Chunk.empty))
        time <- c.downField("time").as[Instant]
      yield StoredMessage(subject, seq, headers, data, time)
    }

/** Page of stream names (paginated `STREAM.NAMES`). */
private[jetstream] final case class StreamNamesResponse(
    streams: List[String],
    total: Int,
    offset: Int
)
private[jetstream] object StreamNamesResponse:
  given Decoder[StreamNamesResponse] = Decoder.instance { c =>
    for
      streams <- c
        .downField("streams")
        .as[Option[List[String]]]
        .map(_.getOrElse(Nil))
      total <- c.downField("total").as[Option[Int]].map(_.getOrElse(0))
      offset <- c.downField("offset").as[Option[Int]].map(_.getOrElse(0))
    yield StreamNamesResponse(streams, total, offset)
  }

/** Page of full stream info (paginated `STREAM.LIST`). */
private[jetstream] final case class StreamListResponse(
    streams: List[StreamInfo],
    total: Int,
    offset: Int
)
private[jetstream] object StreamListResponse:
  given Decoder[StreamListResponse] = Decoder.instance { c =>
    for
      streams <- c
        .downField("streams")
        .as[Option[List[StreamInfo]]]
        .map(_.getOrElse(Nil))
      total <- c.downField("total").as[Option[Int]].map(_.getOrElse(0))
      offset <- c.downField("offset").as[Option[Int]].map(_.getOrElse(0))
    yield StreamListResponse(streams, total, offset)
  }
