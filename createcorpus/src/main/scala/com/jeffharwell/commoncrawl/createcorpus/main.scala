package com.jeffharwell.commoncrawl.createcorpus

import com.typesafe.config.ConfigFactory
import java.net.URL
import java.io.File
import java.util.zip.GZIPInputStream
import java.io.BufferedInputStream
import java.io.BufferedReader
import java.io.InputStreamReader
import java.io.FileInputStream

object Main extends App {
  val conf = ConfigFactory.load()

  /*
   * Try with the local WET file
   */
  val testwetfilename = conf.getString("general.testwetfile")

  val gzipFile: File = getTheFile(testwetfilename)

  var records: Int = 0;
  val parser = Parser(new BufferedInputStream(new FileInputStream(gzipFile)))

  // Count the Records
  records = parser.foldLeft(0) { (acc, i) =>
    acc + 1
  }
  println(s"Found $records")

  // Little function to get the full path of the file in resources or die trying
  def getTheFile(testwetfilename: String): File = {
    val fileResource = Option(this.getClass().getClassLoader().getResource(testwetfilename))

    fileResource match {
      case Some(f) => { return(new File(f.getFile())) }
      case None    => { throw new RuntimeException(s"Didn't find a resource $testwetfilename") }
    }
  }
}
