organization := "com.jeffharwell"

name := "SaveResults"

version := "0.0.1"
publishMavenStyle := true

scalaVersion := "2.11.12"

scalacOptions :=
    scalacOptions.value :+ "-deprecation"

// Realtime Output Please
logBuffered in Test := false

// Adds dependency on typesafe config reader
libraryDependencies += "com.typesafe" % "config" % "1.3.1"
// not needed, at least not right now
//libraryDependencies += "mysql" % "mysql-connector-java" % "6.0.5"

// For the test framework ScalaTest
libraryDependencies += "org.scalactic" %% "scalactic" % "3.0.1" % "test"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1" % "test"

// https://mvnrepository.com/artifact/com.datastax.cassandra/cassandra-driver-core
libraryDependencies += "com.datastax.cassandra" % "cassandra-driver-core" % "3.6.0"

// excludes conf files from jar package
mappings in (Compile, packageBin) ~= { _.filter(!_._1.getName.endsWith(".conf")) }
