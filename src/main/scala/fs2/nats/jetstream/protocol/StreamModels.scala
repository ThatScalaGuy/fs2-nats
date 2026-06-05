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

import com.github.plokhotnyuk.jsoniter_scala.core.*
import com.github.plokhotnyuk.jsoniter_scala.macros.*
import fs2.Chunk
import fs2.nats.protocol.Headers

import java.time.Instant
import scala.concurrent.duration.*

/** A subject transform (`src` → `dest`) applied to a stream or one of its
  * sources.
  */
final case class SubjectTransform(src: String, dest: String)
object SubjectTransform:
  given JsonValueCodec[SubjectTransform] = JsonCodecMaker.make(JsWire.snake)

/** Server-side message republishing: copy messages matching `src` onto `dest`
  * (optionally headers-only).
  */
final case class Republish(
    src: String,
    dest: String,
    headersOnly: Boolean = false
)
object Republish:
  given JsonValueCodec[Republish] = JsonCodecMaker.make(JsWire.snake)

/** Stream placement constraints (preferred cluster and/or tags). */
final case class Placement(
    cluster: Option[String] = None,
    tags: List[String] = Nil
)
object Placement:
  given JsonValueCodec[Placement] = JsonCodecMaker.make(JsWire.snake)

/** A stream mirror or source: another stream whose messages this stream copies,
  * optionally from a start point, filtered, and subject-transformed.
  */
final case class StreamSource(
    name: String,
    optStartSeq: Option[Long] = None,
    optStartTime: Option[Instant] = None,
    filterSubject: Option[String] = None,
    subjectTransforms: List[SubjectTransform] = Nil
)
object StreamSource:
  given JsonValueCodec[StreamSource] = JsonCodecMaker.make(JsWire.snake)

/** Stream configuration. Mirrors the JetStream `StreamConfig` wire model,
  * including mirror/sources/republish/subject-transform/placement and the
  * `sealed` flag.
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
    allowDirect: Boolean = false,
    allowRollupHdrs: Boolean = false,
    denyDelete: Boolean = false,
    discardNewPerSubject: Boolean = false,
    mirror: Option[StreamSource] = None,
    sources: List[StreamSource] = Nil,
    republish: Option[Republish] = None,
    subjectTransform: Option[SubjectTransform] = None,
    placement: Option[Placement] = None,
    sealedStream: Boolean = false
)

object StreamConfig:
  // Hand-written: the encoder always emits scalar fields (including the `-1`
  // sentinels and an empty `subjects`) while omitting `None` durations and
  // description; the decoder treats an absent-or-`0` duration as `None`
  // (JetStream's "unset/unlimited" convention) — neither is expressible via
  // macro defaults.
  given JsonValueCodec[StreamConfig] = new JsonValueCodec[StreamConfig]:
    private val retentionC = summon[JsonValueCodec[RetentionPolicy]]
    private val storageC = summon[JsonValueCodec[StorageType]]
    private val discardC = summon[JsonValueCodec[DiscardPolicy]]
    private val compressionC = summon[JsonValueCodec[StoreCompression]]
    private val streamSourceC = summon[JsonValueCodec[StreamSource]]
    private val republishC = summon[JsonValueCodec[Republish]]
    private val subjectTransformC = summon[JsonValueCodec[SubjectTransform]]
    private val placementC = summon[JsonValueCodec[Placement]]

    def nullValue: StreamConfig = null

    def encodeValue(c: StreamConfig, out: JsonWriter): Unit =
      out.writeObjectStart()
      out.writeKey("name"); out.writeVal(c.name)
      out.writeKey("subjects")
      out.writeArrayStart()
      c.subjects.foreach(out.writeVal)
      out.writeArrayEnd()
      out.writeKey("retention"); retentionC.encodeValue(c.retention, out)
      out.writeKey("storage"); storageC.encodeValue(c.storage, out)
      out.writeKey("max_consumers"); out.writeVal(c.maxConsumers)
      out.writeKey("max_msgs"); out.writeVal(c.maxMsgs)
      out.writeKey("max_bytes"); out.writeVal(c.maxBytes)
      c.maxAge.foreach { d => out.writeKey("max_age"); out.writeVal(d.toNanos) }
      out.writeKey("max_msg_size"); out.writeVal(c.maxMsgSize)
      out.writeKey("max_msgs_per_subject"); out.writeVal(c.maxMsgsPerSubject)
      out.writeKey("num_replicas"); out.writeVal(c.replicas)
      out.writeKey("discard"); discardC.encodeValue(c.discard, out)
      c.duplicateWindow.foreach { d =>
        out.writeKey("duplicate_window"); out.writeVal(d.toNanos)
      }
      c.description.foreach { d =>
        out.writeKey("description"); out.writeVal(d)
      }
      out.writeKey("compression"); compressionC.encodeValue(c.compression, out)
      out.writeKey("allow_direct"); out.writeVal(c.allowDirect)
      // Emit the KV-related flags only when set, so non-KV streams serialize
      // byte-identically to before these fields existed.
      if c.allowRollupHdrs then
        out.writeKey("allow_rollup_hdrs")
        out.writeVal(true)
      if c.denyDelete then
        out.writeKey("deny_delete")
        out.writeVal(true)
      if c.discardNewPerSubject then
        out.writeKey("discard_new_per_subject")
        out.writeVal(true)
      // Advanced fields: emitted only when set, so a config that uses none of
      // them serializes byte-identically to before these fields existed.
      c.mirror.foreach { s =>
        out.writeKey("mirror"); streamSourceC.encodeValue(s, out)
      }
      if c.sources.nonEmpty then
        out.writeKey("sources")
        out.writeArrayStart()
        c.sources.foreach(s => streamSourceC.encodeValue(s, out))
        out.writeArrayEnd()
      c.republish.foreach { r =>
        out.writeKey("republish"); republishC.encodeValue(r, out)
      }
      c.subjectTransform.foreach { st =>
        out.writeKey("subject_transform");
        subjectTransformC.encodeValue(st, out)
      }
      c.placement.foreach { p =>
        out.writeKey("placement"); placementC.encodeValue(p, out)
      }
      if c.sealedStream then
        out.writeKey("sealed")
        out.writeVal(true)
      out.writeObjectEnd()

    def decodeValue(in: JsonReader, default: StreamConfig): StreamConfig =
      if in.isNextToken('{') then
        var name: String = null
        var subjects: List[String] = Nil
        var retention = RetentionPolicy.Limits
        var storage = StorageType.File
        var maxConsumers = -1
        var maxMsgs = -1L
        var maxBytes = -1L
        var maxAge: Option[FiniteDuration] = None
        var maxMsgSize = -1
        var maxMsgsPerSubject = -1L
        var replicas = 1
        var discard = DiscardPolicy.Old
        var duplicateWindow: Option[FiniteDuration] = None
        var description: Option[String] = None
        var compression = StoreCompression.None
        var allowDirect = false
        var allowRollupHdrs = false
        var denyDelete = false
        var discardNewPerSubject = false
        var mirror: Option[StreamSource] = None
        var sources: List[StreamSource] = Nil
        var republish: Option[Republish] = None
        var subjectTransform: Option[SubjectTransform] = None
        var placement: Option[Placement] = None
        var sealedStream = false
        if !in.isNextToken('}') then
          in.rollbackToken()
          var cont = true
          while cont do
            val l = in.readKeyAsCharBuf()
            if in.isCharBufEqualsTo(l, "name") then name = in.readString(null)
            else if in.isCharBufEqualsTo(l, "subjects") then
              subjects = JsRead.stringList(in)
            else if in.isCharBufEqualsTo(l, "retention") then
              retention = retentionC.decodeValue(in, retention)
            else if in.isCharBufEqualsTo(l, "storage") then
              storage = storageC.decodeValue(in, storage)
            else if in.isCharBufEqualsTo(l, "max_consumers") then
              maxConsumers = in.readInt()
            else if in.isCharBufEqualsTo(l, "max_msgs") then
              maxMsgs = in.readLong()
            else if in.isCharBufEqualsTo(l, "max_bytes") then
              maxBytes = in.readLong()
            else if in.isCharBufEqualsTo(l, "max_age") then
              maxAge = JsRead.optDurationNanos(in)
            else if in.isCharBufEqualsTo(l, "max_msg_size") then
              maxMsgSize = in.readInt()
            else if in.isCharBufEqualsTo(l, "max_msgs_per_subject") then
              maxMsgsPerSubject = in.readLong()
            else if in.isCharBufEqualsTo(l, "num_replicas") then
              replicas = in.readInt()
            else if in.isCharBufEqualsTo(l, "discard") then
              discard = discardC.decodeValue(in, discard)
            else if in.isCharBufEqualsTo(l, "duplicate_window") then
              duplicateWindow = JsRead.optDurationNanos(in)
            else if in.isCharBufEqualsTo(l, "description") then
              description = Some(in.readString(null))
            else if in.isCharBufEqualsTo(l, "compression") then
              compression = compressionC.decodeValue(in, compression)
            else if in.isCharBufEqualsTo(l, "allow_direct") then
              allowDirect = in.readBoolean()
            else if in.isCharBufEqualsTo(l, "allow_rollup_hdrs") then
              allowRollupHdrs = in.readBoolean()
            else if in.isCharBufEqualsTo(l, "deny_delete") then
              denyDelete = in.readBoolean()
            else if in.isCharBufEqualsTo(l, "discard_new_per_subject") then
              discardNewPerSubject = in.readBoolean()
            else if in.isCharBufEqualsTo(l, "mirror") then
              mirror = Some(
                streamSourceC.decodeValue(in, streamSourceC.nullValue)
              )
            else if in.isCharBufEqualsTo(l, "sources") then
              sources = JsRead.objectList(in, streamSourceC)
            else if in.isCharBufEqualsTo(l, "republish") then
              republish = Some(republishC.decodeValue(in, republishC.nullValue))
            else if in.isCharBufEqualsTo(l, "subject_transform") then
              subjectTransform = Some(
                subjectTransformC.decodeValue(in, subjectTransformC.nullValue)
              )
            else if in.isCharBufEqualsTo(l, "placement") then
              placement = Some(placementC.decodeValue(in, placementC.nullValue))
            else if in.isCharBufEqualsTo(l, "sealed") then
              sealedStream = in.readBoolean()
            else in.skip()
            cont = in.isNextToken(',')
          if !in.isCurrentToken('}') then in.objectEndOrCommaError()
        StreamConfig(
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
          allowDirect,
          allowRollupHdrs,
          denyDelete,
          discardNewPerSubject,
          mirror,
          sources,
          republish,
          subjectTransform,
          placement,
          sealedStream
        )
      else in.readNullOrTokenError(default, '{')

/** Runtime state of a stream. */
final case class StreamState(
    messages: Long = 0L,
    bytes: Long = 0L,
    firstSeq: Long = 0L,
    lastSeq: Long = 0L,
    consumerCount: Int = 0
)

object StreamState:
  given JsonValueCodec[StreamState] = JsonCodecMaker.make(JsWire.snake)

/** Stream information returned by create/update/info. */
final case class StreamInfo(
    config: StreamConfig,
    state: StreamState,
    created: Instant
)

object StreamInfo:
  given JsonValueCodec[StreamInfo] = JsonCodecMaker.make(JsWire.snake)

/** Options controlling a stream purge. */
final case class PurgeOptions(
    filter: Option[String] = None,
    seq: Option[Long] = None,
    keep: Option[Long] = None
)
object PurgeOptions:
  val all: PurgeOptions = PurgeOptions()

/** Result of a stream purge. */
final case class PurgeResponse(success: Boolean = false, purged: Long = 0L)
object PurgeResponse:
  given JsonValueCodec[PurgeResponse] = JsonCodecMaker.make(JsWire.snake)

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
  // Hand-written: `hdrs` is a base64-encoded header block that must be run
  // through `Headers.parse`, and `data` is base64 bytes — neither is a plain
  // macro-derivable mapping.
  given JsonValueCodec[StoredMessage] = new JsonValueCodec[StoredMessage]:
    def nullValue: StoredMessage = null

    def encodeValue(m: StoredMessage, out: JsonWriter): Unit =
      // Not used in practice (StoredMessage is decode-only), but a complete
      // codec keeps the type usable on both directions.
      out.writeObjectStart()
      out.writeKey("subject"); out.writeVal(m.subject)
      out.writeKey("seq"); out.writeVal(m.seq)
      out.writeKey("data"); out.writeBase64Val(m.data.toArray, doPadding = true)
      out.writeKey("time"); out.writeVal(m.time)
      out.writeObjectEnd()

    def decodeValue(in: JsonReader, default: StoredMessage): StoredMessage =
      if in.isNextToken('{') then
        var subject: String = null
        var seq = 0L
        var headers: Headers = Headers.empty
        var data: Chunk[Byte] = Chunk.empty
        var time: Instant = null
        if !in.isNextToken('}') then
          in.rollbackToken()
          var cont = true
          while cont do
            val l = in.readKeyAsCharBuf()
            if in.isCharBufEqualsTo(l, "subject") then
              subject = in.readString(null)
            else if in.isCharBufEqualsTo(l, "seq") then seq = in.readLong()
            else if in.isCharBufEqualsTo(l, "hdrs") then
              val bytes = in.readBase64AsBytes(null)
              Headers.parse(
                new String(bytes, java.nio.charset.StandardCharsets.UTF_8)
              ) match
                case Right(h)  => headers = h
                case Left(err) => in.decodeError(err)
            else if in.isCharBufEqualsTo(l, "data") then
              data = Chunk.array(in.readBase64AsBytes(Array.emptyByteArray))
            else if in.isCharBufEqualsTo(l, "time") then
              time = in.readInstant(null)
            else in.skip()
            cont = in.isNextToken(',')
          if !in.isCurrentToken('}') then in.objectEndOrCommaError()
        StoredMessage(subject, seq, headers, data, time)
      else in.readNullOrTokenError(default, '{')

/** Page of stream names (paginated `STREAM.NAMES`). */
private[jetstream] final case class StreamNamesResponse(
    streams: List[String] = Nil,
    total: Int = 0,
    offset: Int = 0
)
private[jetstream] object StreamNamesResponse:
  given JsonValueCodec[StreamNamesResponse] = JsonCodecMaker.make(JsWire.snake)

/** Page of full stream info (paginated `STREAM.LIST`). */
private[jetstream] final case class StreamListResponse(
    streams: List[StreamInfo] = Nil,
    total: Int = 0,
    offset: Int = 0
)
private[jetstream] object StreamListResponse:
  given JsonValueCodec[StreamListResponse] = JsonCodecMaker.make(JsWire.snake)
