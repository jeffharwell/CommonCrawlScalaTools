package com.jeffharwell.commoncrawl.createcorpus

import com.typesafe.config.ConfigFactory
import java.io.File
import java.util.zip.GZIPInputStream
import java.io.BufferedInputStream
import java.io.FileInputStream

object Main extends App {
  val conf = ConfigFactory.load()
  val testwetfilename = conf.getString("general.testwetfile")

  val gzipFile: File = getTheFile(testwetfilename)

  val gzInputStream = new GZIPInputStream(new BufferedInputStream(new FileInputStream(gzipFile)))
  val buffer = new Array[ Byte ]( 1024 )
  val parser = new Parser()

  Stream.continually(gzInputStream.read(buffer)).takeWhile(_ != -1).foreach(parser.parseBuffer(buffer, _))
  println("Saw "+parser.getLineCount()+" lines")
  println("Saw "+parser.getRecordCount()+" records")

  // Little function to get the full path of the file in resources or die trying
  def getTheFile(testwetfilename: String): File = {
    val fileResource = Option(this.getClass().getClassLoader().getResource(testwetfilename))

    fileResource match {
      case Some(f) => { return(new File(f.getFile())) }
      case None    => { throw new RuntimeException(s"Didn't find a resource $testwetfilename") }
    }
  }

}
