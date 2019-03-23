package com.jeffharwell.commoncrawl.createcorpus

import scala.io.Source

object parseDocument {
  def main(args: Array[String]): Unit = {
    //var file = "/home/jharwell/Documents/phd/golden_standard/politics/15302326-94bf-4b7e-b8ef-fdec63f5b1ac.txt"
    var file = "/home/jharwell/Documents/phd/golden_standard/politics/100adb2c-a2a4-4c54-9a63-e606109a090c.txt"
    //var file = "/home/jharwell/Documents/phd/golden_standard/test_block.txt" 

    // https://stackoverflow.com/questions/1284423/read-entire-file-in-scala
    //
    // I have some reservations here. I Spark I would definitely be handing this
    // class a chunk of text, not an array of lines or the like. So I think this
    // is the best approximation and leads towards writing a class that will
    // work in Spark with modification.
    val source = scala.io.Source.fromFile(file)
    val document = try source.mkString finally source.close()

    // Do preparation
    var parser = new PrepareDocument(document)
    parser.prepare()


  }
}
