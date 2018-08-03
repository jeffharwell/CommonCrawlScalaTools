organization := "com.jeffharwell"

name := "WARCParser"

version := "0.0.8"
publishMavenStyle := true

//scalaVersion := "2.12.0"
// Spark 1.6.7 like Scala 2.10.5
scalaVersion := "2.10.5"

// Realtime Output Please
logBuffered in Test := false

// Adds dependency on typesafe config reader
libraryDependencies += "com.typesafe" % "config" % "1.3.1"
// not needed, at least not right now
//libraryDependencies += "mysql" % "mysql-connector-java" % "6.0.5"

// For the test framework ScalaTest
libraryDependencies += "org.scalactic" %% "scalactic" % "3.0.1" % "test"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1" % "test"

// excludes conf files from jar package
mappings in (Compile, packageBin) ~= { _.filter(!_._1.getName.endsWith(".conf")) }
