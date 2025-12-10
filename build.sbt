lazy val V = new {
  val scala3 = "3.3.7"
  val catsEffect = "3.6.3"
  val fs2 = "3.12.2"
  val circe = "0.14.15"
  val munit = "1.2.1"
  val munitCatsEffect = "2.1.0"
  val scalaCheck = "1.19.0"
  val munitScalaCheck = "1.2.0"
}

ThisBuild / tlBaseVersion := "0.1"
ThisBuild / organization := "de.thatscalaguy"
ThisBuild / organizationName := "ThatScalaGuy"
ThisBuild / startYear := Some(2025)
ThisBuild / licenses := Seq(License.Apache2)
ThisBuild / developers ++= List(
  tlGitHubDev("ThatScalaGuy", "Sven Herrmann")
)

ThisBuild / githubWorkflowJavaVersions := Seq(
  JavaSpec.temurin("8"),
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
      "io.circe" %% "circe-core" % V.circe,
      "io.circe" %% "circe-generic" % V.circe,
      "io.circe" %% "circe-parser" % V.circe,
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
    Test / fork := true
  )
