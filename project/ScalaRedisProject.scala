import sbt._
import Keys._

object ScalaRedisProject extends Build
{
  import Resolvers._
  lazy val root = Project("RedisReact", file(".")) settings(coreSettings : _*)

  lazy val commonSettings: Seq[Setting[_]] = Seq(
    organization := "net.debasishg",
    version := "0.6-SNAPSHOT",
    scalaVersion := "2.10.4",
    crossScalaVersions := Seq("2.10.4", "2.11.1"),
    scalacOptions := Seq("-deprecation", "-unchecked", "-feature", "-language:postfixOps"),
    resolvers ++= Seq(akkaRelease, akkaSnapshot, sprayJson)
  )

  lazy val coreSettings = commonSettings ++ Seq(
    name := "RedisReact",
    libraryDependencies :=
        Seq(
          "com.typesafe.akka" %%  "akka-actor"      % "2.3.3",
          "com.typesafe.akka" %%  "akka-slf4j"      % "2.3.3"     % "provided",
          "commons-pool"      %   "commons-pool"    % "1.6",
          "org.slf4j"         %   "slf4j-api"       % "1.7.7"     % "provided",
          "ch.qos.logback"    %   "logback-classic" % "1.1.2"     % "provided",
          "junit"             %   "junit"           % "4.11"      % "test",
          "org.scalatest"     %%  "scalatest"       % "2.1.6"     % "test",
          "com.typesafe.akka" %%  "akka-testkit"    % "2.3.3"     % "test",

          // Third-party serialization libraries
          //"net.liftweb" %%  "lift-json"      % "2.5.1" % "provided, test",
          "org.json4s"  %%  "json4s-native"  % "3.2.10" % "provided, test",
          "org.json4s"  %%  "json4s-jackson" % "3.2.10" % "provided, test",
          "io.spray"    %%  "spray-json"     % "1.2.6" % "provided, test"
        ),
    parallelExecution in Test := false,
    publishTo <<= version { (v: String) =>
      val nexus = "https://oss.sonatype.org/" 
      if (v.trim.endsWith("SNAPSHOT")) Some("snapshots" at nexus + "content/repositories/snapshots")
      else Some("releases" at nexus + "service/local/staging/deploy/maven2") 
    },
    credentials += Credentials(Path.userHome / ".sbt" / "sonatype.credentials"),
    publishMavenStyle := true,
    publishArtifact in Test := false,
    pomIncludeRepository := { repo => false },
    pomExtra := (
      <url>https://github.com/debasishg/scala-redis-nb</url>
      <licenses>
        <license>
          <name>Apache 2.0 License</name>
          <url>http://www.apache.org/licenses/LICENSE-2.0.html</url>
          <distribution>repo</distribution>
        </license>
      </licenses>
      <scm>
        <url>git@github.com:debasishg/scala-redis-nb.git</url>
        <connection>scm:git:git@github.com:debasishg/scala-redis-nb.git</connection>
      </scm>
      <developers>
        <developer>
          <id>debasishg</id>
          <name>Debasish Ghosh</name>
          <url>http://debasishg.blogspot.com</url>
        </developer>
        <developer>
          <id>guersam</id>
          <name>Jisoo Park</name>
        </developer>
      </developers>),
    unmanagedResources in Compile <+= baseDirectory map { _ / "LICENSE" }
  )
}

object Resolvers {
  val akkaRelease = "typesafe release repo" at "http://repo.typesafe.com/typesafe/releases/"
  val akkaSnapshot = "typesafe snapshot repo" at "http://repo.typesafe.com/typesafe/snapshots/"
  val sprayJson = "spray" at "http://repo.spray.io/"
}
