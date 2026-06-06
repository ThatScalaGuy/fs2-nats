import com.typesafe.tools.mima.core.*

lazy val V = new {
  val scala3 = "3.3.7"
  val catsEffect = "3.7.0"
  val fs2 = "3.13.0"
  val jsoniter = "2.38.14"
  val bouncyCastle = "1.84"
  val munit = "1.2.4"
  val munitCatsEffect = "2.2.0"
  val scalaCheck = "1.19.0"
  val munitScalaCheck = "1.3.0"
}

ThisBuild / tlBaseVersion := "0.2"
ThisBuild / organization := "de.thatscalaguy"
ThisBuild / organizationName := "ThatScalaGuy"
ThisBuild / startYear := Some(2025)
ThisBuild / licenses := Seq(License.Apache2)
ThisBuild / developers ++= List(
  tlGitHubDev("ThatScalaGuy", "Sven Herrmann")
)

ThisBuild / githubWorkflowJavaVersions := Seq(
  JavaSpec.temurin("11"),
  JavaSpec.temurin("17"),
  JavaSpec.temurin("21"),
  JavaSpec.temurin("25")
)

ThisBuild / scalaVersion := V.scala3
ThisBuild / scalacOptions ++= Seq(
  "-Wunused:all"
)

lazy val root = project
  .in(file("."))
  .settings(
    name := "fs2-nats",
    scalaVersion := V.scala3,
    libraryDependencies ++= Seq(
      "org.typelevel" %% "cats-effect" % V.catsEffect,
      "co.fs2" %% "fs2-core" % V.fs2,
      "co.fs2" %% "fs2-io" % V.fs2,
      "com.github.plokhotnyuk.jsoniter-scala" %% "jsoniter-scala-core" % V.jsoniter,
      "com.github.plokhotnyuk.jsoniter-scala" %% "jsoniter-scala-macros" % V.jsoniter % "compile-internal",
      "org.bouncycastle" % "bcprov-jdk18on" % V.bouncyCastle,
      "org.scalameta" %% "munit" % V.munit % Test,
      "org.typelevel" %% "munit-cats-effect" % V.munitCatsEffect % Test,
      "org.scalacheck" %% "scalacheck" % V.scalaCheck % Test,
      "org.scalameta" %% "munit-scalacheck" % V.munitScalaCheck % Test
    ),
    Test / fork := true,
    Test / parallelExecution := false,
    // The Tier 2/3 performance refactor changed the signatures of object-private
    // implementation classes. These are not part of the public API; MiMa only
    // sees them because Scala emits nested private classes as separate classfiles.
    mimaBinaryIssueFilters ++= Seq(
      ProblemFilters.exclude[IncompatibleMethTypeProblem](
        "fs2.nats.publish.Publisher#PublisherImpl.this"
      ),
      ProblemFilters.exclude[DirectMissingMethodProblem](
        "fs2.nats.subscriptions.SubscriptionManager#InternalSubscription.this"
      ),
      ProblemFilters.exclude[DirectMissingMethodProblem](
        "fs2.nats.subscriptions.SubscriptionManager#InternalSubscription.remainingRef"
      ),
      ProblemFilters.exclude[DirectMissingMethodProblem](
        "fs2.nats.subscriptions.SubscriptionManager#InternalSubscription.activeRef"
      ),
      // JetStream P1: the request/reply primitive adds `request` to the
      // NatsClient trait and surfaces the status line code + description on
      // HMsgFrame and NatsMessage (new fields with defaults). These are
      // additive public-API changes that MiMa flags as binary-incompatible.
      ProblemFilters.exclude[ReversedMissingMethodProblem](
        "fs2.nats.client.NatsClient.request"
      ),
      // JetStream P2: adds the `jetStream` context factory to NatsClient.
      ProblemFilters.exclude[ReversedMissingMethodProblem](
        "fs2.nats.client.NatsClient.jetStream"
      ),
      ProblemFilters.exclude[DirectMissingMethodProblem](
        "fs2.nats.client.NatsClient#NatsClientImpl.this"
      ),
      ProblemFilters.exclude[DirectMissingMethodProblem](
        "fs2.nats.protocol.NatsFrame#HMsgFrame.apply"
      ),
      ProblemFilters.exclude[DirectMissingMethodProblem](
        "fs2.nats.protocol.NatsFrame#HMsgFrame.copy"
      ),
      ProblemFilters.exclude[DirectMissingMethodProblem](
        "fs2.nats.protocol.NatsFrame#HMsgFrame.this"
      ),
      ProblemFilters.exclude[IncompatibleResultTypeProblem](
        "fs2.nats.protocol.NatsFrame#HMsgFrame._6"
      ),
      ProblemFilters.exclude[IncompatibleResultTypeProblem](
        "fs2.nats.protocol.NatsFrame#HMsgFrame.copy$default$6"
      ),
      ProblemFilters.exclude[DirectMissingMethodProblem](
        "fs2.nats.subscriptions.NatsMessage.apply"
      ),
      ProblemFilters.exclude[DirectMissingMethodProblem](
        "fs2.nats.subscriptions.NatsMessage.copy"
      ),
      ProblemFilters.exclude[DirectMissingMethodProblem](
        "fs2.nats.subscriptions.NatsMessage.this"
      ),
      // KV: adds the key-value factory/management methods to the JetStream
      // trait. Additive abstract methods MiMa flags as binary-incompatible.
      ProblemFilters.exclude[ReversedMissingMethodProblem](
        "fs2.nats.jetstream.JetStream.createKeyValue"
      ),
      ProblemFilters.exclude[ReversedMissingMethodProblem](
        "fs2.nats.jetstream.JetStream.keyValue"
      ),
      ProblemFilters.exclude[ReversedMissingMethodProblem](
        "fs2.nats.jetstream.JetStream.deleteKeyValue"
      ),
      ProblemFilters.exclude[ReversedMissingMethodProblem](
        "fs2.nats.jetstream.JetStream.keyValueStatus"
      ),
      ProblemFilters.exclude[ReversedMissingMethodProblem](
        "fs2.nats.jetstream.JetStream.keyValueNames"
      ),
      // Ordered consumer: adds the `subscribeOrdered` factory to the JetStream
      // trait (additive abstract method).
      ProblemFilters.exclude[ReversedMissingMethodProblem](
        "fs2.nats.jetstream.JetStream.subscribeOrdered"
      ),
      // Object Store: adds the object-store factory/management methods to the
      // JetStream trait. Additive abstract methods MiMa flags as incompatible.
      ProblemFilters.exclude[ReversedMissingMethodProblem](
        "fs2.nats.jetstream.JetStream.createObjectStore"
      ),
      ProblemFilters.exclude[ReversedMissingMethodProblem](
        "fs2.nats.jetstream.JetStream.objectStore"
      ),
      ProblemFilters.exclude[ReversedMissingMethodProblem](
        "fs2.nats.jetstream.JetStream.deleteObjectStore"
      ),
      ProblemFilters.exclude[ReversedMissingMethodProblem](
        "fs2.nats.jetstream.JetStream.objectStoreStatus"
      ),
      ProblemFilters.exclude[ReversedMissingMethodProblem](
        "fs2.nats.jetstream.JetStream.objectStoreNames"
      ),
      // KV: StreamConfig gains the allow_rollup_hdrs / deny_delete /
      // discard_new_per_subject flags (defaulted false). New case-class fields
      // change the synthesized apply/copy/constructor signatures.
      ProblemFilters.exclude[DirectMissingMethodProblem](
        "fs2.nats.jetstream.protocol.StreamConfig.apply"
      ),
      ProblemFilters.exclude[DirectMissingMethodProblem](
        "fs2.nats.jetstream.protocol.StreamConfig.copy"
      ),
      ProblemFilters.exclude[DirectMissingMethodProblem](
        "fs2.nats.jetstream.protocol.StreamConfig.this"
      )
    )
  )

lazy val integration = project
  .in(file("integration"))
  .dependsOn(root)
  .settings(
    name := "fs2-nats-integration",
    scalaVersion := V.scala3,
    libraryDependencies ++= Seq(
      "org.scalameta" %% "munit" % V.munit % Test,
      "org.typelevel" %% "munit-cats-effect" % V.munitCatsEffect % Test
    ),
    Test / fork := true,
    // Integration tests share a single NATS broker; some (reconnect) restart it,
    // so run suites sequentially rather than in parallel.
    Test / parallelExecution := false
  )

lazy val benchmarks = project
  .in(file("benchmarks"))
  .dependsOn(root)
  .enablePlugins(JmhPlugin, NoPublishPlugin)
  .settings(
    name := "fs2-nats-benchmarks",
    scalaVersion := V.scala3,
    // JMH generates Java sources compiled with an obsolete --release 8; under CI
    // sbt-typelevel turns warnings into errors. This is a NoPublish dev tool, so
    // don't fail its build on those warnings.
    tlFatalWarnings := false,
    Compile / javacOptions ~= (_.filterNot(_ == "-Werror"))
  )
