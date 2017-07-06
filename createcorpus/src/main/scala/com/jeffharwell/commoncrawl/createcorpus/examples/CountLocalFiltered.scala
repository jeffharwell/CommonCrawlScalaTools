package com.jeffharwell.commoncrawl.createcorpus.examples

/* Common Crawl Classes */
import com.jeffharwell.commoncrawl.createcorpus.Parser
import com.jeffharwell.commoncrawl.createcorpus.WARCRecord
import com.jeffharwell.commoncrawl.createcorpus.MyWARCFilter

/* Other Classes */
import scala.collection.mutable.ListBuffer
import java.io.BufferedInputStream
import java.io.FileInputStream
import java.io.File
//import java.io.InputStream


//import MyWARCCategorizer

object CountLocalFiltered {
  def main(args: Array[String]): Unit = {
    // Get our local file 
    val filename = "CC-MAIN-20161202170900-00009-ip-10-31-129-80.ec2.internal.warc.wet.gz"
    val fileurl = this.getClass().getClassLoader().getResource(filename)

    println("URL is "+fileurl)
    val f = new File(fileurl.getFile())

    /*
     * Example 1:
     *
     * Parse a compressed WET file and count the WARC records
     */

    // Create the parser
    val parser_no_filter = new Parser(new BufferedInputStream(
      new FileInputStream(f)))

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
    val parser = new Parser(new BufferedInputStream(
      new FileInputStream(f)))

    // Initialize a ListBuffer to hold the records
    val records = new ListBuffer[WARCRecord]()

    // Apply the filter using withFilter
    parser.withFilter(myfilter(_)).foreach((wc: WARCRecord) => records += wc)

    println("Found "+records.size+" records from "+filename+" using filter MyWARCFilter")
    println("\nThe content of the first record is:\n")
    println(records(0).fields("Content"))
  }
}
