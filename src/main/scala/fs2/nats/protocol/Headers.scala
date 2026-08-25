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
import scala.collection.immutable.VectorBuilder

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
    * Every protocol-defined header name and value is ASCII, and for ASCII
    * `String.length` is already the UTF-8 byte count — so the block is measured
    * once and then written straight into the array that is returned. The
    * `StringBuilder` route this replaces materialized it three times over: the
    * builder's own array (regrown twice on a typical block), the `String` that
    * `toString` copies out of it, and a third copy from `getBytes`, which on a
    * latin1-compact String is an `Arrays.copyOf` — the JDK never hands out the
    * String's own array.
    *
    * `asciiSize` runs to completion before anything is allocated, so a
    * non-ASCII key or value falls back to the encoder rather than narrowing a
    * multi-byte char into an array sized as if it were one byte.
    *
    * @return
    *   Chunk of bytes in NATS header format
    */
  def toBytes: Chunk[Byte] =
    if entries.isEmpty then Chunk.empty
    else
      val size = asciiSize
      if size < 0 then Headers.encodeUtf8(entries)
      else
        val arr = new Array[Byte](size)
        System.arraycopy(
          Headers.VersionLine,
          0,
          arr,
          0,
          Headers.VersionLine.length
        )
        var off = Headers.VersionLine.length
        var i = 0
        val n = entries.size
        while i < n do
          val e = entries(i)
          off = Headers.putAscii(arr, off, e._1)
          arr(off) = Headers.Colon
          arr(off + 1) = Headers.Space
          off = Headers.putAscii(arr, off + 2, e._2)
          arr(off) = Headers.Cr
          arr(off + 1) = Headers.Lf
          off += 2
          i += 1
        arr(off) = Headers.Cr
        arr(off + 1) = Headers.Lf
        Chunk.array(arr)

  /** Get the serialized byte length of these headers.
    *
    * Counts the block instead of building it. Nothing in the client calls this
    * — `Publisher` reads `.size` off the `Chunk` it is about to send anyway —
    * so serializing a whole block to read one Int was pure waste. The ASCII
    * branch is the same arithmetic `toBytes` sizes its array with, so the two
    * cannot drift; only a non-ASCII block still has to ask the encoder.
    *
    * @return
    *   The byte length when serialized
    */
  def byteLength: Int =
    if entries.isEmpty then 0
    else
      val size = asciiSize
      // Straight to the encoder, not via `toBytes`, which would re-run the
      // scan that already answered `-1`.
      if size < 0 then Headers.encodeUtf8(entries).size else size

  /** Exact serialized size when every key and value is ASCII, `-1` when one is
    * not — in that case only the UTF-8 encoder knows the answer.
    */
  private def asciiSize: Int =
    var size = Headers.VersionLine.length + 2
    var i = 0
    val n = entries.size
    while i < n && size >= 0 do
      val e = entries(i)
      if Headers.isAscii(e._1) && Headers.isAscii(e._2) then
        size += e._1.length + 2 + e._2.length + 2
      else size = -1
      i += 1
    size

object Headers:
  /** NATS header version string */
  val Version: String = "NATS/1.0"

  /** CRLF line terminator */
  val CRLF: String = "\r\n"

  /** Empty headers instance */
  val empty: Headers = Headers(Vector.empty)

  private val VersionLine: Array[Byte] =
    (Version + CRLF).getBytes(StandardCharsets.US_ASCII)
  private val Colon: Byte = ':'.toByte
  private val Space: Byte = ' '.toByte
  private val Cr: Byte = '\r'.toByte
  private val Lf: Byte = '\n'.toByte

  /** A `null` counts as non-ASCII so it takes the `StringBuilder` fallback,
    * which appends the four chars `null` — nothing in the client can produce a
    * null key or value, but that is what serializing one did before and this
    * rewrite is not the place to change it.
    */
  private def isAscii(s: String): Boolean =
    (s ne null) && {
      var i = 0
      val n = s.length
      while i < n && s.charAt(i) <= 0x7f do i += 1
      i == n
    }

  /** Narrowing a char to a byte is exact only for `<= 0x7f`; callers must have
    * run `isAscii` first.
    */
  private def putAscii(arr: Array[Byte], off: Int, s: String): Int =
    var i = 0
    val n = s.length
    while i < n do
      arr(off + i) = s.charAt(i).toByte
      i += 1
    off + n

  /** The pre-rewrite path, kept verbatim for blocks with a non-ASCII key or
    * value: `getBytes` maps an unpaired surrogate to a single `'?'`, and
    * reproducing that by hand is more semantics than a path this cold is worth.
    */
  private def encodeUtf8(entries: Vector[(String, String)]): Chunk[Byte] =
    val sb = new StringBuilder
    sb.append(Version)
    sb.append(CRLF)
    entries.foreach { case (k, v) =>
      sb.append(k)
      sb.append(": ")
      sb.append(v)
      sb.append(CRLF)
    }
    sb.append(CRLF)
    Chunk.array(sb.toString.getBytes(StandardCharsets.UTF_8))

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
    parseWithStatus(bytes).map(_._3)

  /** Parse headers from NATS/1.0 format string.
    *
    * @param str
    *   The header string to parse
    * @return
    *   Either a parse error or the parsed Headers
    */
  def parse(str: String): Either[String, Headers] =
    parseWithStatus(str).map(_._3)

  /** Parse headers from NATS/1.0 format bytes, extracting the status code and
    * description if present. Control messages appear as "NATS/1.0 503" or
    * "NATS/1.0 100 Idle Heartbeat", etc.
    *
    * @param bytes
    *   The header bytes to parse
    * @return
    *   Either a parse error or tuple of (optional status code, optional status
    *   description, Headers)
    */
  def parseWithStatus(
      bytes: Chunk[Byte]
  ): Either[String, (Option[Int], Option[String], Headers)] =
    val str = new String(bytes.toArray, StandardCharsets.UTF_8)
    parseWithStatus(str)

  /** Parse the header block held in `buf[from, until)`.
    *
    * The parser owns its carry buffer, so `ProtocolParser.buildHMsg` can hand
    * the header region over directly instead of first copying it into a
    * throwaway `Chunk` whose only use was being turned back into a String.
    * Decoding a sub-range is byte-for-byte the same as decoding a copy of it,
    * including the U+FFFD substitutions for malformed input. Unlike the
    * `copyOfRange` it replaces, a range reaching past `buf` throws rather than
    * zero-padding; callers must pass a range they have already buffered, which
    * `buildHMsg` does — it only runs once the whole frame is in the carry.
    */
  private[nats] def parseWithStatus(
      buf: Array[Byte],
      from: Int,
      until: Int
  ): Either[String, (Option[Int], Option[String], Headers)] =
    parseWithStatus(
      new String(buf, from, until - from, StandardCharsets.UTF_8)
    )

  /** Parse headers from NATS/1.0 format string, extracting the status code and
    * description if present.
    *
    * Hot path — this runs once per HMSG delivery, i.e. every JetStream message,
    * every KV direct get and every watch event. The block is walked with
    * `indexOf(CRLF, _)` plus a bounded colon scan and field boundaries are
    * carried as index pairs, so the only strings ever materialized are the ones
    * that escape: the status description and each key and value.
    * `split("\r\n", -1)` walked the same block but compiled a fresh
    * `java.util.regex.Pattern` on every call ("\r\n" is two characters, so it
    * misses `String.split`'s single-char fast path), then built a line array, a
    * Vector of lines, one `Either` per line and two `collect` passes on top of
    * that.
    *
    * The two whitespace notions here are deliberately different and must stay
    * that way: `trimFrom`/`trimUntil` strip what `String.trim` strips (every
    * char `<= ' '`), while the status line splits at the first
    * `Character.isWhitespace`, a wider and partly non-ASCII set.
    *
    * @param str
    *   The header string to parse
    * @return
    *   Either a parse error or tuple of (optional status code, optional status
    *   description, Headers)
    */
  def parseWithStatus(
      str: String
  ): Either[String, (Option[Int], Option[String], Headers)] =
    // "NATS/1.0" contains no CR, so the first CRLF can only start at index 8
    // or later: testing the prefix on the whole block is the same test as
    // testing it on the version line.
    if !str.startsWith(Version) then
      Left(s"Invalid NATS headers: missing or invalid version line")
    else
      val len = str.length
      val vEnd = lineEnd(str, 0, len)

      // The version line is "NATS/1.0 <code> <description>" for control
      // messages (e.g. "NATS/1.0 100 Idle Heartbeat"). Split the leading
      // status code from the trailing description so callers can match on
      // both; a bare "NATS/1.0 503" yields a code with no description.
      var statusCode: Option[Int] = None
      var statusDescription: Option[String] = None
      if vEnd > Version.length then
        val rs = trimFrom(str, Version.length, vEnd)
        val re = trimUntil(str, rs, vEnd)
        if rs < re then
          var w = rs
          while w < re && !Character.isWhitespace(str.charAt(w)) do w += 1
          // `w == re` is "no separator": the whole remainder is the code, as
          // before. `w == rs` is a remainder opening with a char that is
          // whitespace to `isWhitespace` but not to `trim` (U+2003 and
          // friends); that leaves an empty code range, and "".toIntOption is
          // None — also as before.
          statusCode = str.substring(rs, w).toIntOption
          if w < re then
            val ds = trimFrom(str, w + 1, re)
            val de = trimUntil(str, ds, re)
            if ds < de then statusDescription = Some(str.substring(ds, de))

      // Header lines run to the first blank line, and everything after it is
      // ignored — malformed lines included — exactly as
      // `lines.tail.takeWhile(_.nonEmpty)` did. A block that ends without a
      // trailing CRLF still yields its last line.
      var pos = if vEnd >= len then len else vEnd + 2
      // A control block (idle heartbeat, 404/408/409, 503) carries a status
      // line and no entries at all, and VectorBuilder allocates its 32-slot
      // array in its constructor — so it is built only once there is something
      // to put in it.
      var entries: VectorBuilder[(String, String)] = null
      var errors: StringBuilder = null
      var scanning = true
      while scanning do
        val e = lineEnd(str, pos, len)
        if e == pos then scanning = false // blank line, or end of block
        else
          val c = colonIn(str, pos, e)
          if c < 0 then
            // A malformed line is the only one ever materialized whole, and it
            // is reported raw — untrimmed — as before. All bad lines are
            // collected, so this cannot short-circuit.
            val line = str.substring(pos, e)
            if errors == null then errors = new StringBuilder
            else errors.append("; ")
            errors.append(s"Invalid header line (no colon): $line")
          else
            val ks = trimFrom(str, pos, c)
            val vs = trimFrom(str, c + 1, e)
            if entries == null then entries = new VectorBuilder
            entries.addOne(
              (
                str.substring(ks, trimUntil(str, ks, c)),
                str.substring(vs, trimUntil(str, vs, e))
              )
            )
          if e >= len then scanning = false // last line, no trailing CRLF
          else pos = e + 2

      if errors != null then Left(errors.toString)
      else if entries == null then
        Right((statusCode, statusDescription, Headers.empty))
      else Right((statusCode, statusDescription, Headers(entries.result())))

  /** Index of the CRLF that ends the line starting at `from`, or `len` when the
    * block ends without one. Only the pair terminates a line — a lone CR or a
    * lone LF is an ordinary character, exactly as `split("\r\n", -1)` had it.
    */
  private def lineEnd(s: String, from: Int, len: Int): Int =
    val i = s.indexOf(CRLF, from)
    if i < 0 then len else i

  /** Index of the first `':'` in `s[from, until)`, or -1 when the line has
    * none.
    *
    * Scans rather than calling `indexOf(':', from)`: `indexOf` would run past
    * `until` to the next colon anywhere in the block, so a block of colon-less
    * lines would cost O(lines * block), which the old line-at-a-time
    * `line.indexOf(':')` did not. Nothing caps the header length a server may
    * declare, so that is remotely reachable.
    */
  private def colonIn(s: String, from: Int, until: Int): Int =
    var i = from
    while i < until && s.charAt(i) != ':' do i += 1
    if i < until then i else -1

  /** First index in `s[from, until)` that `String.trim` would keep. `trim`
    * strips every char `<= ' '` — all C0 controls, not just spaces — and `Char`
    * is unsigned, so this comparison is exactly `trim`'s predicate.
    */
  private def trimFrom(s: String, from: Int, until: Int): Int =
    var i = from
    while i < until && s.charAt(i) <= ' ' do i += 1
    i

  /** Exclusive end of `s[from, until)` after `String.trim`'s trailing strip. */
  private def trimUntil(s: String, from: Int, until: Int): Int =
    var i = until
    while i > from && s.charAt(i - 1) <= ' ' do i -= 1
    i
