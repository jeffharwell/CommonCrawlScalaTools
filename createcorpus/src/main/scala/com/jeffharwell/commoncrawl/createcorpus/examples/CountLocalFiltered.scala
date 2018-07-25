package com.jeffharwell.commoncrawl.createcorpus.examples

/* Common Crawl Classes */
import com.jeffharwell.commoncrawl.createcorpus.Parser
import com.jeffharwell.commoncrawl.createcorpus.WARCRecord
import com.jeffharwell.commoncrawl.createcorpus.MyWARCFilter
import com.jeffharwell.commoncrawl.createcorpus.MyWARCCategorizer

/* Other Classes */
import scala.collection.mutable.ListBuffer
import java.io.BufferedInputStream
import java.io.FileInputStream
import java.io.File
import java.net.URL

/* For the examples */
//import scala.io.StdIn.readLine // scala 2.11 and greater


//import MyWARCCategorizer

object CountLocalFiltered {
  def main(args: Array[String]): Unit = {


    def examples = {

      /*
       * Example 1:
       *
       * Parse a compressed WET file and count the WARC records
       */

      // Get our local file 
      val filename = "CC-MAIN-20161202170900-00009-ip-10-31-129-80.ec2.internal.warc.wet.gz"
      val fileurl = this.getClass().getClassLoader().getResource(filename)

      println("URL is "+fileurl)
      val f = new File(fileurl.getFile())

      // Create the parser
      val parser_no_filter = Parser(new BufferedInputStream(new FileInputStream(f)))

      // Count the Records
      val record_count = parser_no_filter.foldLeft(0) { (acc, i) =>
        acc + 1
      }
      println(s"Parser without a filter found $record_count WARC Records")


      /*
       * Example 2: 
       *
       * Read a compressed WET file, filter the content on the fly, 
       * and create a ListBuffer with the matching records. Then print out the content
       * of the first record.
       */

      // Create the Filter
      val myfilter = new MyWARCFilter()

      // Create the parser
      val parser = Parser(new BufferedInputStream(new FileInputStream(f)))

      // Initialize a ListBuffer to hold the records
      val records = new ListBuffer[WARCRecord]()

      // Apply the filter using withFilter
      parser.withFilter(myfilter(_)).foreach((wc: WARCRecord) => records += wc)

      println("Found "+records.size+" records from "+filename+" using filter MyWARCFilter")
      println("\nThe content of the first record is:\n")
      println(records(0).fields("Content"))
    }

    //examples

    /*
     * Example 3: 
     *
     * Using the classifier
     */
    def example3 = {
      val filename = "classifier_example_file.wet.gz"
      val fileurl = this.getClass().getClassLoader().getResource(filename)

      println("URL is "+fileurl)
      val f = new File(fileurl.getFile())

      // Create the Filter
      val myfilter = new MyWARCFilter()
      myfilter.setMinMentions(1)

      // Create the categorizer
      val c: MyWARCCategorizer = new MyWARCCategorizer(1)

      // Create the parser
      val parser = Parser(new BufferedInputStream(new FileInputStream(f)), c)

      // Initialize a ListBuffer to hold the records
      val records = new ListBuffer[WARCRecord]()

      // Apply the filter using withFilter
      parser.withFilter(myfilter(_)).foreach((wc: WARCRecord) => records += wc)
      // Without the filter
      //parser.foreach((wc: WARCRecord) => records += wc)

      // Count the Records
      val record_count = parser.foldLeft(0) { (acc, i) =>
        acc + 1
      }

      // Length of Content String
      def contentsize(content: Option[String]): Int = {
          content match {
              case Some(c) => c.size
              case _ => 0
          }
      }

      // Count occurances of a sub string
      // https://www.rosettacode.org/wiki/Count_occurrences_of_a_substring#Using_Regular_Expressions
      def countSubstring(str: Option[String], substr: String): Int = {
        str match {
          case Some(c) => { substr.r.findAllMatchIn(c.toUpperCase).length +
                            substr.r.findAllMatchIn(c.toLowerCase).length
                          }
          case _ => 0
        }
      }

      def mycontentfilter(content: Option[String]): Boolean = {
          content match {
              case Some(c) => c.contains("asthma")
              case _ => false
          }
      }

      /*
      println(s"Parser with a filter found ${records.size} WARC Records")
      records.foreach(r => {
        var content_size = contentsize(r.getContent())
        if (r.get("WARC-Record-ID") == Some("<urn:uuid:a0f94225-2d1d-4033-a7bd-6b18f1dcd95e>")) {
          if (mycontentfilter(r.getContent())) {
            println("Contains the word asthma")
          } else {
            println("Does not contain the word asthma")
          }
        }
      })
      */

      // All WARC records that contain asthma
      records.foreach( r => {
        var num_a = countSubstring(r.getContent(), "a") + countSubstring(r.getContent(), "e")
        var content_size = contentsize(r.getContent())

        println(s"WARC Record: ${r.get("WARC-Record-ID")} $content_size, a's and e's $num_a")
        println(myfilter(r))
        println(r.getCategories())
        r.getContent() match {
          case Some(content) => println(c.getCategories(content))
          case _ => println(c.getCategories(""))
        }
      })


      /*

      println("Found "+records.size+" records from "+filename+" using filter MyWARCFilter")

      // Dump all records that contain the word asthma
      def mycontentfilter(content: Option[String]): Boolean = {
          content match {
              case Some(c) => c.contains("asthma")
              case _ => false
          }
      }
      // Count occurances of a sub string
      // https://www.rosettacode.org/wiki/Count_occurrences_of_a_substring#Using_Regular_Expressions
      def countSubstring(str: Option[String], substr: String): Int = {
        str match {
          case Some(c) => substr.r.findAllMatchIn(c).length
          case _ => 0
        }
      }

      val l = records.filter(y => mycontentfilter(y.getContent()))
      records.foreach(y => {
        println(s"${y.get("WARC-Record-ID")}")
        val numoccurances = countSubstring(y.getContent(), "asthma")
        println(s"$numoccurances occurance(s) of the string asthma")
        println(y.getCategories())
      })
      */
    }

    example3


  /*
   * Example 4:
   *
   * Read and filter a compressed WET archive straight from the web.
   */

    def example4 = {
      //val input = scala.io.StdIn.readLine() // for Scala 2.11 and greater
      var input = readLine("Do you want to parse a file straight from AWS? (y/N): ")
      if (input != "y") {
        println("Done with example 3")
      } else {
        // This is the path from the wet.paths file
        var filepath = "crawl-data/CC-MAIN-2016-50/segments/1480698540409.8/wet/CC-MAIN-20161202170900-00007-ip-10-31-129-80.ec2.internal.warc.wet.gz"
        val urlbase = "https://commoncrawl.s3.amazonaws.com/"
        var url = new URL(urlbase+filepath)
        //println("URL is "+fileurl)
        //val u = new File(fileurl.getFile())

        // Create the Filter
        val myfilter = new MyWARCFilter()

        // Create the parser
        var parser_from_aws = Parser(new BufferedInputStream(url.openStream()))

       // Initialize a ListBuffer to hold the records
        var records_from_aws = new ListBuffer[WARCRecord]()

        val collection = parser_from_aws.withFilter(myfilter(_)).foreach((wc: WARCRecord) => records_from_aws += wc)
        println("Found "+records_from_aws.size+" records from "+url+" using filter MyWARCFilter")
        println("\nThe content of the first record is:\n")
        println(records_from_aws(0).fields("Content"))
      }
    }

    example4

  /*
   * Example 5:
   *
   * Read and classify a compressed WET archive from the web.
   */
    def example5 = {
      val input = readLine("Do you want to parse a file straight from AWS? (y/N): ")
      if (input != "y") {
        println("Done with example 4")
      } else {
        // This is the path from the wet.paths file
        var filepath = "crawl-data/CC-MAIN-2016-50/segments/1480698540409.8/wet/CC-MAIN-20161202170900-00007-ip-10-31-129-80.ec2.internal.warc.wet.gz"
        val urlbase = "https://commoncrawl.s3.amazonaws.com/"
        var url = new URL(urlbase+filepath)

        // Create the categorizer
        val c: MyWARCCategorizer = new MyWARCCategorizer(2)

        // Create the Filter
        val myfilter = new MyWARCFilter()


        // Create the parser
        val parser_from_aws_2 = Parser(new BufferedInputStream(url.openStream()), c)

        val records_from_aws = new ListBuffer[WARCRecord]()

        parser_from_aws_2.withFilter(myfilter(_)).foreach((wc: WARCRecord) => records_from_aws += wc)

        def mycontentfilter(content: Option[String]): Boolean = {
            content match {
                case Some(c) => c.contains("asthma")
                case _ => false
            }
        }
        val l = records_from_aws.filter(y => mycontentfilter(y.getContent()))
        l.foreach(y => println(y.getCategories()))
      }

    }

    example5

  } // end main

} // end object
