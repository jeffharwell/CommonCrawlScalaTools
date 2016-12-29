//import sbt._

organization := "com.jeffharwell"

name := "WetPathLoader"

version := "0.0.1"

scalaVersion := "2.12.0"

publishMavenStyle := true

libraryDependencies += "com.typesafe" % "config" % "1.3.1"
libraryDependencies += "mysql" % "mysql-connector-java" % "6.0.5"

// This works to exclude application.conf from compile
// this one sets: inspect compile:excludeFilter
// excludeFilter in (Compile) := HiddenFileFilter || "*.conf"

// excludeFilter in (Compile, packageBin) := HiddenFileFilter || "*.conf"

// Modifies inspect compile:packageBin::mappings
//   of type scala.collection.Seq[scala.Tuple2[java.io.File, java.lang.String]]
// ~= is a method that transforms a setting .. it disappeared from the 0.13 documentation
//   but you can find it in the 0.12.2 documentation in the "More Kinds of Settings" page
//   under the "Transforming a value: ~=" section.
//   (http://www.scala-sbt.org/0.12.2/docs/Getting-Started/More-About-Settings.html)
//   There was talk on Stackoverflow that this stopped working in 13.9 but 
//     it was still working in 13.13 so I'm not sure what happened in the docs
//   ._1 is the syntax to access the first value in the tuple .. so
//     mappings in grabs the collection, _.filter says filter the collection and
//     _._1 grabs the first element of every tuple in the collection
mappings in (Compile, packageBin) ~= { _.filter(!_._1.getName.endsWith(".conf")) }

// Uugh, so much work
// To dump
// sbt
// clean
// consolePackage
// (mappings in (Compile,packageBin)).eval
