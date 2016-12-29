package com.jeffharwell.commoncrawl.wetpathloader

import com.typesafe.config.ConfigFactory
import scala.io.BufferedSource
import java.nio.file.{Paths, Files}

object Main extends App {

  val conf = ConfigFactory.load()
  val driver = conf.getString("dbconnection.driver")
  val url = conf.getString("dbconnection.url")
  val username = conf.getString("dbconnection.username")
  val password = conf.getString("dbconnection.password")

  val loader = new WetPathLoader(driver, url, username, password)

  val inputSource = getBufferedSource("wet.paths.201612")

  val wpl = new WetPathLoader(driver, url, username, password)
  wpl.loadPaths(inputSource)

  /*
   * A bit of flexibility for the WET Paths file. First we will check in the 
   * resources of the project and see if that is available. Then we will check
   * if it exists on the filesystem. If neither matches then we throw an error.
   *
   * Scala is a bit tricky - the scala.io.Source methods throw a Java NPE error
   *   if you touch the return value, with no exception, if the file the referenced
   *   doesn't exist. Potentially I should not use them at all and instead write 
   *   it with Java'ish code.
   */
  def getBufferedSource(sourceString: String): BufferedSource = {

    /* First check the resources location */
    // this is Java that returns null so we have to use Option to handle the null
    val fileResource = Option(this.getClass().getClassLoader().getResource(sourceString))

    fileResource match {
      case Some(f) => return(scala.io.Source.fromResource(sourceString))
      case None            => println(s"Didn't fined a resource $sourceString")
    }

    /* Now check the filesystem */
    // this is Java that returns a boolean, so we can get away with an if statement
    val path = Paths.get(sourceString)
    if (Files.exists(path)) {
      // Found it
      return(scala.io.Source.fromFile(sourceString))
    }

    /* Neither Worked, throw an exception and die */
    // there has got to be a better way to write this
    throw new RuntimeException("Couldn't find the file")
  }

}

