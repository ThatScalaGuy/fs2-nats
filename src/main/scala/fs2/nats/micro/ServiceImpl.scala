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

package fs2.nats.micro

import cats.effect.std.Supervisor
import cats.effect.syntax.all.*
import cats.effect.{Async, Ref, Resource}
import cats.syntax.all.*
import com.github.plokhotnyuk.jsoniter_scala.core.writeToArray
import fs2.nats.client.NatsClient
import fs2.nats.micro.protocol.*
import fs2.nats.protocol.Headers
import fs2.nats.subscriptions.NatsMessage
import fs2.nats.util.Tokens
import fs2.{Chunk, Stream}

import java.time.Instant
import scala.concurrent.duration.*

/** Server runtime shared by the endpoint loops, the control plane and the
  * [[NatsService]] accessors.
  */
private[micro] final class ServiceRuntime[F[_]](
    client: NatsClient[F],
    config: ServiceConfig,
    val id: String,
    handlers: List[MicroHandler.Impl[F, Any, Any, Any, Any]],
    state: Ref[F, ServiceImpl.State]
)(using F: Async[F]):

  import ServiceImpl.{Counters, State}

  private[micro] def effectiveQueueGroup(rpc: Rpc[?, ?, ?, ?]): String =
    rpc.queueGroup.getOrElse(config.queueGroup)

  /** The 9 ADR-32 discovery subjects, no queue group. Built by concatenation
    * because `$SRV` would trip string interpolation.
    */
  private[micro] val controlSubjects: List[(String, String)] =
    for
      verb <- List("PING", "INFO", "STATS")
      suffix <- List("", "." + config.name, "." + config.name + "." + id)
    yield (verb, "$SRV." + verb + suffix)

  // ---- NatsService accessors -------------------------------------------

  def info: F[ServiceInfo] =
    F.pure(
      new ServiceInfo(
        config.name,
        id,
        config.version,
        config.description,
        config.metadata,
        handlers.map { h =>
          new EndpointInfo(
            h.rpc.name,
            h.rpc.subject.render,
            effectiveQueueGroup(h.rpc),
            endpointMetadata(h.rpc)
          )
        }
      )
    )

  def stats: F[ServiceStats] =
    state.get.map { s =>
      new ServiceStats(
        config.name,
        id,
        config.version,
        s.started,
        handlers.map { h =>
          val c = s.counters.getOrElse(h.rpc.name, Counters.zero)
          new EndpointStats(
            h.rpc.name,
            h.rpc.subject.render,
            effectiveQueueGroup(h.rpc),
            c.numRequests,
            c.numErrors,
            c.lastError,
            c.processingTimeNanos.nanos,
            c.averageNanos.nanos
          )
        }
      )
    }

  def reset: F[Unit] =
    F.realTime.flatMap { d =>
      state.set(
        State(
          Instant.ofEpochMilli(d.toMillis),
          ServiceImpl.zeroCounters(handlers)
        )
      )
    }

  /** INFO endpoint metadata: payload schemas merged under the explicit
    * `Rpc.metadata` (explicit keys win on collision).
    */
  private def endpointMetadata(rpc: Rpc[?, ?, ?, ?]): Map[String, String] =
    rpc.in.schema.map("request_schema" -> _).toMap ++
      rpc.out.schema.map("response_schema" -> _).toMap ++
      rpc.metadata

  // ---- request path ----------------------------------------------------

  private[micro] def endpointLoop(
      h: MicroHandler.Impl[F, Any, Any, Any, Any],
      msgs: Stream[F, NatsMessage]
  ): Stream[F, Unit] =
    msgs.parEvalMapUnordered(config.maxConcurrent) { msg =>
      // Backstop: the per-message effect must be total or the endpoint dies.
      // F.defer also catches synchronous throws from user codecs/handlers
      // that escape handleOne's own guards.
      F.defer(handleOne(h, msg)).handleErrorWith(_ => F.unit)
    }

  private def handleOne(
      h: MicroHandler.Impl[F, Any, Any, Any, Any],
      msg: NatsMessage
  ): F[Unit] =
    val rpc = h.rpc
    guarded(rpc.subject.extract(msg.subject)) match
      case Left(e)  => rejected(rpc.name, msg, s"invalid subject params: $e")
      case Right(p) =>
        guarded(rpc.in.decode(msg.payload)) match
          case Left(e) =>
            rejected(rpc.name, msg, s"invalid request payload: $e")
          case Right(i) =>
            F.defer(h.run(p, msg.headers, i)).attempt.timed.flatMap {
              case (elapsed, Right(Right(reply))) =>
                Either.catchNonFatal(rpc.out.encode(reply.value)) match
                  case Right(bytes) =>
                    publishSuccess(msg, bytes, reply.headers).flatMap {
                      case Right(()) => record(rpc.name, elapsed.toNanos, None)
                      case Left(t)   =>
                        record(
                          rpc.name,
                          elapsed.toNanos,
                          Some(s"reply publish failed: ${t.getMessage}")
                        )
                    }
                  case Left(t) =>
                    failed(
                      rpc.name,
                      msg,
                      500,
                      s"response encode failed: ${describe(t)}",
                      elapsed.toNanos
                    )
              case (elapsed, Right(Left(e))) =>
                Either.catchNonFatal(rpc.err.encode(e)) match
                  case Right((code, desc)) =>
                    failed(rpc.name, msg, code, desc, elapsed.toNanos)
                  case Left(t) =>
                    failed(
                      rpc.name,
                      msg,
                      500,
                      s"error encode failed: ${describe(t)}",
                      elapsed.toNanos
                    )
              case (elapsed, Left(t)) =>
                failed(rpc.name, msg, 500, describe(t), elapsed.toNanos)
            }

  /** User codecs may throw instead of returning `Left`. */
  private def guarded[A](e: => Either[String, A]): Either[String, A] =
    try e
    catch
      case scala.util.control.NonFatal(t) =>
        Left(s"codec threw: ${describe(t)}")

  private def describe(t: Throwable): String =
    Option(t.getMessage).getOrElse(t.getClass.getName)

  /** Request rejected before the handler ran (bad params/payload). */
  private def rejected(name: String, msg: NatsMessage, desc: String): F[Unit] =
    failed(name, msg, 400, desc, elapsedNanos = 0L)

  private def failed(
      name: String,
      msg: NatsMessage,
      code: Int,
      desc: String,
      elapsedNanos: Long
  ): F[Unit] =
    val clean = sanitize(desc)
    publishError(msg, code, clean) *>
      record(name, elapsedNanos, Some(s"$code $clean"))

  /** Header values must be a single line: descriptions come from uncontrolled
    * text (exception messages, codec errors) and embedded CR/LF would corrupt
    * or inject into the reply's header block. Keep the first line, blank out
    * remaining control chars, cap the length.
    */
  private def sanitize(desc: String): String =
    desc
      .takeWhile(c => c != '\n' && c != '\r')
      .map(c => if c.isControl then ' ' else c)
      .take(256)

  /** Reply header keys and values are handler-supplied and often echo request
    * data; a CR or LF inside one would start a new line in the reply's header
    * block, letting a request forge e.g. `Nats-Service-Error-Code` and turn its
    * own success into an error at the caller. Blank out the control characters
    * but keep the length: unlike the error descriptions `sanitize` trims, these
    * are values the handler chose to send.
    */
  private def sanitizeHeaders(h: Headers): Headers =
    if h.entries.forall((k, v) => !hasControl(k) && !hasControl(v)) then h
    else Headers(h.entries.map((k, v) => (blankControl(k), blankControl(v))))

  private def hasControl(s: String): Boolean = s.exists(_.isControl)

  private def blankControl(s: String): String =
    s.map(c => if c.isControl then ' ' else c)

  private def publishSuccess(
      msg: NatsMessage,
      bytes: Chunk[Byte],
      headers: Headers
  ): F[Either[Throwable, Unit]] =
    msg.replyTo match
      case Some(rt) =>
        client.publish(rt, bytes, sanitizeHeaders(headers)).attempt
      case None => F.pure(Right(()))

  private def publishError(msg: NatsMessage, code: Int, desc: String): F[Unit] =
    msg.replyTo match
      case Some(rt) =>
        client
          .publish(
            rt,
            Chunk.empty,
            Headers(
              MicroHeaders.ErrorCode -> code.toString,
              MicroHeaders.Error -> desc
            )
          )
          .attempt
          .void
      case None => F.unit

  private def record(
      name: String,
      elapsedNanos: Long,
      error: Option[String]
  ): F[Unit] =
    state.update { s =>
      s.copy(counters =
        s.counters.updatedWith(name)(c =>
          Some(c.getOrElse(Counters.zero).record(elapsedNanos, error))
        )
      )
    }

  // ---- control plane ---------------------------------------------------

  private[micro] def controlLoop(
      verb: String,
      msgs: Stream[F, NatsMessage]
  ): Stream[F, Unit] =
    msgs.evalMap(msg => respond(verb, msg).handleErrorWith(_ => F.unit))

  private def respond(verb: String, msg: NatsMessage): F[Unit] =
    msg.replyTo match
      case None     => F.unit
      case Some(rt) =>
        val payload: F[Array[Byte]] = verb match
          case "PING" => F.delay(writeToArray(pingResponse))
          case "INFO" => F.delay(writeToArray(infoResponse))
          case _      => state.get.map(s => writeToArray(statsResponse(s)))
        payload.flatMap(b => client.publish(rt, Chunk.array(b)).attempt.void)

  private def pingResponse: PingResponse =
    PingResponse(
      name = config.name,
      id = id,
      version = config.version,
      metadata = config.metadata
    )

  private def infoResponse: InfoResponse =
    InfoResponse(
      name = config.name,
      id = id,
      version = config.version,
      metadata = config.metadata,
      description = config.description,
      endpoints = handlers.map { h =>
        WireEndpointInfo(
          h.rpc.name,
          h.rpc.subject.render,
          effectiveQueueGroup(h.rpc),
          endpointMetadata(h.rpc)
        )
      }
    )

  private def statsResponse(s: State): StatsResponse =
    StatsResponse(
      name = config.name,
      id = id,
      version = config.version,
      metadata = config.metadata,
      started = s.started,
      endpoints = handlers.map { h =>
        val c = s.counters.getOrElse(h.rpc.name, Counters.zero)
        WireEndpointStats(
          h.rpc.name,
          h.rpc.subject.render,
          effectiveQueueGroup(h.rpc),
          c.numRequests,
          c.numErrors,
          c.lastError,
          c.processingTimeNanos,
          c.averageNanos
        )
      }
    )

private[micro] object ServiceImpl:

  private val NameRegex = "[A-Za-z0-9\\-_]+".r
  private val SemVerRegex =
    """\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?(?:\+[0-9A-Za-z.-]+)?""".r

  private[micro] final case class Counters(
      numRequests: Long,
      numErrors: Long,
      lastError: Option[String],
      processingTimeNanos: Long
  ):
    def record(elapsedNanos: Long, error: Option[String]): Counters =
      Counters(
        numRequests + 1,
        numErrors + (if error.isDefined then 1L else 0L),
        error.orElse(lastError),
        processingTimeNanos + elapsedNanos
      )

    def averageNanos: Long =
      if numRequests > 0 then processingTimeNanos / numRequests else 0L

  private[micro] object Counters:
    val zero: Counters = Counters(0L, 0L, None, 0L)

  private[micro] final case class State(
      started: Instant,
      counters: Map[String, Counters]
  )

  private[micro] def zeroCounters[F[_]](
      handlers: List[MicroHandler.Impl[F, Any, Any, Any, Any]]
  ): Map[String, Counters] =
    handlers.map(h => h.rpc.name -> Counters.zero).toMap

  def resource[F[_]](
      client: NatsClient[F],
      config: ServiceConfig,
      handlers: List[MicroHandler[F]]
  )(using F: Async[F]): Resource[F, NatsService[F]] =
    // MicroHandler has exactly one (sealed) implementation; the cast only
    // erases the endpoint's type parameters, whose values always flow through
    // the matching Rpc's codecs.
    val impls =
      handlers.map(_.asInstanceOf[MicroHandler.Impl[F, Any, Any, Any, Any]])
    for
      _ <- Resource.eval(validate(config, impls))
      id <- Resource.eval(Tokens.randomInboxId[F]())
      started <- Resource.eval(
        F.realTime.map(d => Instant.ofEpochMilli(d.toMillis))
      )
      state <- Resource.eval(
        Ref.of[F, State](State(started, zeroCounters(impls)))
      )
      runtime = new ServiceRuntime[F](client, config, id, impls, state)
      endpointSubs <- impls.traverse { h =>
        client
          .subscribe(
            h.rpc.subject.render,
            Some(runtime.effectiveQueueGroup(h.rpc))
          )
          .map(h -> _)
      }
      controlSubs <- runtime.controlSubjects.traverse { case (verb, subject) =>
        client.subscribe(subject, None).map(verb -> _)
      }
      sup <- Supervisor[F]
      _ <- Resource.eval(endpointSubs.traverse_ { case (h, msgs) =>
        sup.supervise(runtime.endpointLoop(h, msgs).compile.drain).void
      })
      _ <- Resource.eval(controlSubs.traverse_ { case (verb, msgs) =>
        sup.supervise(runtime.controlLoop(verb, msgs).compile.drain).void
      })
    yield new NatsServiceImpl[F](runtime)

  private def validate[F[_]](
      config: ServiceConfig,
      handlers: List[MicroHandler.Impl[F, Any, Any, Any, Any]]
  )(using F: Async[F]): F[Unit] =
    F.delay {
      require(
        NameRegex.matches(config.name),
        s"invalid service name '${config.name}': must match [A-Za-z0-9-_]+"
      )
      require(
        SemVerRegex.matches(config.version),
        s"invalid service version '${config.version}': must be SemVer"
      )
      require(handlers.nonEmpty, "handler list must not be empty")
      handlers.foreach { h =>
        require(
          NameRegex.matches(h.rpc.name),
          s"invalid endpoint name '${h.rpc.name}': must match [A-Za-z0-9-_]+"
        )
      }
      val duplicates =
        handlers.groupBy(_.rpc.name).collect {
          case (name, hs) if hs.sizeIs > 1 => name
        }
      require(
        duplicates.isEmpty,
        s"duplicate endpoint names: ${duplicates.mkString(", ")}"
      )
    }
