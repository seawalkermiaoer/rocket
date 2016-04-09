import sbt._
import Keys._

name := "Hummer"

organization := "com.iflytek"

version := "0.1.0"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq(
  "io.spray"  % "spray-json_2.10" % "1.3.1",
  "org.scalatest" % "scalatest_2.10" % "2.2.1" % "test"
)

//dependencyOverrides += "com.typesafe" % "config" % "1.2.1"

resolvers ++= Seq(
  "Akka Repository" at "http://repo.akka.io/releases/",
  "Typesafe repository" at "http://repo.typesafe.com/typesafe/releases/",
  "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"
)


//sbt -Dhadoop.version=1.0.4 package
    