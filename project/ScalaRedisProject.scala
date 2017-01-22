import sbt._
import Keys._

object ScalaRedisProject extends Build
{
  import Resolvers._
  lazy val root = Project("RedisReact", file(".")) settings(coreSettings : _*)

  lazy val commonSettings: Seq[Setting[_]] = Seq(
    organization := "net.debasishg",
    version := "0.9",
    scalaVersion := "2.11.8",
    crossScalaVersions := Seq("2.10.4", "2.11.8", "2.12.0"),
    scalacOptions := Seq("-deprecation", "-unchecked", "-feature", "-language:postfixOps"),
    resolvers ++= Seq(akkaRelease, akkaSnapshot, sprayJson)
  )

  val akkaVersion = Def.setting{
    if(scalaVersion.value startsWith "2.12") "2.4.12"
    else "2.3.4"
  }
  val json4sVersion = "3.5.0"

  lazy val coreSettings = commonSettings ++ Seq(
    name := "RedisReact",
    libraryDependencies :=
        Seq(
          "com.typesafe.akka" %%  "akka-actor"      % akkaVersion.value,
          "com.typesafe.akka" %%  "akka-slf4j"      % akkaVersion.value % "provided",
          "commons-pool"      %   "commons-pool"    % "1.6",
          "org.slf4j"         %   "slf4j-api"       % "1.7.7"     % "provided",
          "ch.qos.logback"    %   "logback-classic" % "1.1.2"     % "provided",
          "junit"             %   "junit"           % "4.11"      % "test",
          "org.scalatest"     %%  "scalatest"       % "3.0.0"     % "test",
          "com.typesafe.akka" %%  "akka-testkit"    % akkaVersion.value % "test",

          // Third-party serialization libraries
          //"net.liftweb" %%  "lift-json"      % "2.5.1" % "provided, test",
          "org.json4s"  %%  "json4s-native"  % json4sVersion % "provided, test",
          "org.json4s"  %%  "json4s-jackson" % json4sVersion % "provided, test",
          "io.spray"    %%  "spray-json"     % "1.3.2" % "provided, test"
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
