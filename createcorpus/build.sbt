organization := "com.jeffharwell"

name := "CreateCorpus"

version := "0.0.1"

scalaVersion := "2.12.0"

// Realtime Output Please
logBuffered in Test := false

// Adds dependency on typesafe config reader
libraryDependencies += "com.typesafe" % "config" % "1.3.1"
libraryDependencies += "mysql" % "mysql-connector-java" % "6.0.5"

// For the test framework ScalaTest
libraryDependencies += "org.scalactic" %% "scalactic" % "3.0.1"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1" % "test"

// excludes conf files from jar package
mappings in (Compile, packageBin) ~= { _.filter(!_._1.getName.endsWith(".conf")) }
