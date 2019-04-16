package com.jeffharwell.commoncrawl.createcorpus

import scala.io.Source

object prepareDocument {
  def main(args: Array[String]): Unit = {
    //var file = "/home/jharwell/Documents/phd/golden_standard/politics/15302326-94bf-4b7e-b8ef-fdec63f5b1ac.txt"
    //var file = "/home/jharwell/Documents/phd/golden_standard/politics/100adb2c-a2a4-4c54-9a63-e606109a090c.txt"
    //var file = "/home/jharwell/Documents/phd/golden_standard/test_block.txt" 
    //var file = "/home/jharwell/Documents/phd/golden_standard/politics/055f0ada-2b4a-4567-b4bc-71efff22f930.txt"
    //var file = "/home/jharwell/Documents/phd/golden_standard/politics/733de7c6-44e0-488c-8f95-16b3b11ef173.txt"
    //var file = "/home/jharwell/Documents/phd/golden_standard/politics/826646da-df8c-4c49-963e-491151a033ce.txt"
    var file = "/home/jharwell/Documents/phd/golden_standard/politics/7dfad428-7ab7-40cd-95c3-e5ff8ff7de6d.txt"

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
    parser.setDebug()
    println(parser.prepare())
  }
}
