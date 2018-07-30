import sbt._

name := "kafka-hierarchy-prototype"
version := "0.1-SNAPSHOT"
organization := "com.bazaarvoice"
scalaVersion := "2.12.6"
fork in Test := true

resolvers ++= Seq(
  Resolver.mavenLocal,
  "Bazaarvoice" at "https://repo.bazaarvoice.com:443/nexus/content/groups/bazaarvoice",
  "Apache Development Snapshot Repository" at "https://repository.apache.org/content/repositories/snapshots/",
  "Sonatype OSS Releases" at "https://oss.sonatype.org/content/repositories/releases",
  "Confluent" at "http://packages.confluent.io/maven/"
)

credentials += Credentials(Path.userHome / ".sbt" / ".credentials")

scalacOptions ++= Seq(
  "-Xfatal-warnings",
  "-deprecation",
  "-feature",
  "-language:_",
  "-unchecked",
  "-Xlint",
  "-Yno-adapted-args",
  "-Ywarn-dead-code"
)

libraryDependencies ++= Seq(
  "org.apache.kafka" % "kafka-streams" % "1.1.0",
  "org.apache.kafka" % "kafka-clients" % "1.1.0",
  "com.lightbend" %% "kafka-streams-scala" % "0.2.1",
  "net.manub" %% "scalatest-embedded-kafka" % "1.1.0-kafka1.1-nosr"
)