name := "swatch"

organization := "com.mirkocaserta.swatch"

version := "1.0.2-SNAPSHOT"

// scalaVersion := "2.12.8"
// 2020.4 Flatmap
scalaVersion := "2.11.12"

scalacOptions ++= Seq("-encoding", "UTF-8", "-deprecation", "-unchecked", "-feature")

scalaSource in Compile := baseDirectory.value / "src" / "main"

lazy val root = (project in file(".")).enablePlugins(PlayScala)

libraryDependencies += "org.scalatestplus.play" %% "scalatestplus-play" % "4.0.1" % Test

/*
libraryDependencies ++= Seq(
  "ch.qos.logback" % "logback-classic" % "1.0.11" % "test",
  "com.typesafe.akka" %% "akka-kernel" % "2.1.2",
  "com.typesafe.akka" %% "akka-slf4j" % "2.1.2",
  "com.typesafe.akka" %% "akka-testkit" % "2.1.2" % "test",
  "org.scalatest" %% "scalatest" % "1.9.1" % "test",
  "org.specs2" %% "specs2" % "1.13" % "test"
)
*/


