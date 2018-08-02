// For reference see:
// https://spark.apache.org/docs/latest/quick-start.html
// https://medium.com/luckspark/scala-spark-tutorial-1-hello-world-7e66747faec
// https://stackoverflow.com/questions/10773319/sbt-doesnt-find-file-in-local-maven-repository-although-its-there

organization := "com.jeffharwell"

name := "CreateCorpus"

version := "0.0.1"
publishMavenStyle := true

// Spark 1.6.7 like Scala 2.10.5
scalaVersion := "2.10.5"

// Realtime Output Please
logBuffered in Test := false

// The WARCParser Library
libraryDependencies += "com.jeffharwell" %% "warcparser" % "0.0.7"

// Spark library
// "provided" means that this dependency will be provided by the runtime environment
// (in this case Spark) so it does not need to be include when sbt-assembly is creating
// the "fat jar"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.6.3" % "provided"

// For the test framework ScalaTest
libraryDependencies += "org.scalactic" %% "scalactic" % "3.0.1" % "test"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1" % "test"

// excludes conf files from jar package
mappings in (Compile, packageBin) ~= { _.filter(!_._1.getName.endsWith(".conf")) }
