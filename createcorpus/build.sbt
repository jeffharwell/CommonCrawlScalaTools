organization := "com.jeffharwell"

name := "CreateCorpus"

version := "0.0.1"

scalaVersion := "2.12.0"

// Adds dependency on typesafe config reader
libraryDependencies += "com.typesafe" % "config" % "1.3.1"
libraryDependencies += "mysql" % "mysql-connector-java" % "6.0.5"

// excludes conf files from jar package
mappings in (Compile, packageBin) ~= { _.filter(!_._1.getName.endsWith(".conf")) }
