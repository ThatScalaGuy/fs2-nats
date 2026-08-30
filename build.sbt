lazy val V = new {
  val scala3 = "3.3.8"
  val catsEffect = "3.7.0"
  val fs2 = "3.13.0"
  val jsoniter = "2.40.1"
  val bouncyCastle = "1.85.2"
  val munit = "1.3.5"
  val munitCatsEffect = "2.2.0"
  val scalaCheck = "1.19.0"
  val munitScalaCheck = "1.3.0"
}

ThisBuild / tlBaseVersion := "0.4"
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
    Test / parallelExecution := false
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

lazy val docs = project
  .in(file("site"))
  .enablePlugins(TypelevelSitePlugin)
  .dependsOn(root)
  .settings(
    name := "fs2-nats-docs",
    scalaVersion := V.scala3,
    // micro.md derives jsoniter codecs in its snippets; same scope as in root.
    libraryDependencies += "com.github.plokhotnyuk.jsoniter-scala" %% "jsoniter-scala-macros" % V.jsoniter % "compile-internal",
    // Read markdown sources from the repo-root `docs/` dir (deterministic; the
    // plugin otherwise inherits MdocPlugin's project-relative `site/docs` default).
    mdocIn := (ThisBuild / baseDirectory).value / "docs",
    // mdoc snippets trip `-Wunused:all`, which CI promotes to errors. Same
    // rationale as the benchmarks module — don't fail the docs build on those.
    tlFatalWarnings := false,
    // Adds an "API" link in the site navigation pointing at the published Scaladoc.
    tlSiteApiUrl := Some(
      url("https://www.javadoc.io/doc/de.thatscalaguy/fs2-nats_3/latest/")
    )
  )
