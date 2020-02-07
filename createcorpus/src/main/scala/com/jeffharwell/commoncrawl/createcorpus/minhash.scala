package com.jeffharwell.commoncrawl.createcorpus

import scala.io.Source
import scala.util.hashing.MurmurHash3
import scala.collection.mutable.ListBuffer

object minhash {
  def main(args: Array[String]): Unit = {
    //var file = "/home/jharwell/Documents/phd/golden_standard/politics/15302326-94bf-4b7e-b8ef-fdec63f5b1ac.txt"
    //var file = "/home/jharwell/Documents/phd/golden_standard/politics/100adb2c-a2a4-4c54-9a63-e606109a090c.txt"
    //var file = "/home/jharwell/Documents/phd/golden_standard/test_block.txt" 
    //var file = "/home/jharwell/Documents/phd/golden_standard/politics/055f0ada-2b4a-4567-b4bc-71efff22f930.txt"
    //var file = "/home/jharwell/Documents/phd/golden_standard/politics/733de7c6-44e0-488c-8f95-16b3b11ef173.txt"
    //var file = "/home/jharwell/Documents/phd/golden_standard/politics/826646da-df8c-4c49-963e-491151a033ce.txt"
    var file = "/home/jharwell/Documents/phd/golden_standard/politics_v20190421/opitslinkfest.blogspot.com-1dc71fe7-4d21-4740-99c0-b2c81fd01dc2.txt"

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
    var tokens = parser.tokenize_line(document)
    println(tokens)

    def shingle(tokens: List[String], shingle_size: Int): ListBuffer[Int] = {
      var shingles: ListBuffer[Int] = new ListBuffer[Int]()
      var murmurseed = 0
      // https://github.com/mitmedialab/DataBasic/blob/master/nltk_data/corpora/stopwords/english
      var python_stop_words = List("i","me","my","myself","we","our","ours","ourselves","you","your","yours","yourself","yourselves","he","him",
                                   "his","himself","she","her","hers","herself","it","its","itself","they","them","their","theirs","themselves",
                                   "what","which","who","whom","this","that","these","those","am","is","are","was","were","be","been","being","have",
                                   "has","had","having","do","does","did","doing","a","an","the","and","but","if","or","because","as","until","while",
                                   "of","at","by","for","with","about","against","between","into","through","during","before","after","above","below",
                                   "to","from","up","down","in","out","on","off","over","under","again","further","then","once","here","there","when",
                                   "where","why","how","all","any","both","each","few","more","most","other","some","such","no","nor","not","only",
                                   "own","same","so","than","too","very","s","t","can","will","just","don","should","now")

      def getShingle(tokens: List[String]): Int = {
        var shingleList: List[String] = tokens.slice(0, shingle_size)
        MurmurHash3.listHash(shingleList, murmurseed)
      }

      @scala.annotation.tailrec
      def processTokens(tokens: List[String], shingles: ListBuffer[Int]): ListBuffer[Int] = {
        if (tokens.size < shingle_size) {
          shingles
        } else {
          if (python_stop_words.contains(tokens.head.toLowerCase())) {
            var shingleList: List[String] = tokens.slice(0, shingle_size)
            println(shingleList.mkString(" "))
            shingles.append(MurmurHash3.listHash(shingleList, murmurseed))
          }
          processTokens(tokens.tail, shingles)
        }
      }
      processTokens(tokens, shingles).distinct
    }

    var fingerprint = shingle(tokens.toList, 9)
    println(fingerprint)
    println(tokens.size)
    println(fingerprint.size)
  }
}
