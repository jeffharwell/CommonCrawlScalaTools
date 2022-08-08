package com.jeffharwell.commoncrawl.warcparser.examples

/* Common Crawl Classes */
import com.jeffharwell.commoncrawl.warcparser.Parser
import com.jeffharwell.commoncrawl.warcparser.WARCRecord
import com.jeffharwell.commoncrawl.warcparser.MyWARCStreamFilter
import com.jeffharwell.commoncrawl.warcparser.MyWARCTopicFilter
import com.jeffharwell.commoncrawl.warcparser.WARCTopicFilter
import com.jeffharwell.commoncrawl.warcparser.EmptyTopicFilter
import com.jeffharwell.commoncrawl.warcparser.WARCConversion
import com.jeffharwell.commoncrawl.warcparser.ParserTrigger

/* Other Classes */
import scala.collection.mutable.ListBuffer
import java.io.BufferedInputStream
import java.io.InputStream
import java.io.FileInputStream
import java.io.File
import java.net.URL

object TestFilterOnArchive {
  def main(args: Array[String]): Unit = {

    println("Reading and filtering a compressed WET archive straight from the web.")
    // This is the path from the wet.paths file
    //var filepath = "crawl-data/CC-MAIN-2016-50/segments/1480698540409.8/wet/CC-MAIN-20161202170900-00007-ip-10-31-129-80.ec2.internal.warc.wet.gz"
    //var filepath = "crawl-data/CC-MAIN-2016-50/segments/1480698542657.90/wet/CC-MAIN-20161202170902-00359-ip-10-31-129-80.ec2.internal.warc.wet.gz"
    var filepath = "crawl-data/CC-MAIN-2016-50/segments/1480698541140.30/wet/CC-MAIN-20161202170901-00007-ip-10-31-129-80.ec2.internal.warc.wet.gz"
    val urlbase = "https://commoncrawl.s3.amazonaws.com/"
    var url = new URL(urlbase+filepath)

    // Create the Filter
    val myfilter = new MyWARCStreamFilter()
    myfilter.setDebugMessageLength(700000)
    myfilter.setMaxChunksToCheck(10000)
    myfilter.setDebug()

    // Create our finish trigger
    val myFinishTrigger = new MyParserTrigger

    // Create the parser
    var urlconnection = url.openConnection()
    urlconnection.setReadTimeout(60000)
    urlconnection.setConnectTimeout(60000)
    var parser = ParserTest(new BufferedInputStream(urlconnection.getInputStream()),1000000)
    parser.setDebug()
	parser.addFinishTrigger(myFinishTrigger)

    var filtered_parser = parser.withFilter(myfilter(_))

    val records = ListBuffer[WARCRecord]()
    filtered_parser.foreach((wc: WARCRecord) => records += wc)

    val loopcheck = 1000000 // use this to prevent infinite loops
    var looptries = 0 // this is the counter

	// run the parser to state 6, at this point we have found the corrupt record and 
    // are scanning for any new WARC conversion records that we can parse
    /*
    looptries = 0
    while (parser.getParserState() != "Final" && looptries < loopcheck) {
      looptries += 1
      parser.nextFSAStep()
      if (looptries % 1000 == 0) {
        println(s"We have run ${looptries} call to nextFSAStep")
        println(s"We have run ${parser.getSteps()} parser steps")
        println(s"We have parsed ${parser.getRecordCount()} record(s)")
      }
    }
    */

    // We are done, did we trip the loopcheck? Does the parser think there are more records to come?
    println(s"Finished: looptries = ${looptries}")
    println(s"Parser reports that hasNext is ${parser.hasNext()}")

    // Does the finish trigger have anything interesting to add?
    println(s"Finish trigger was called ${myFinishTrigger.getTimesCalled()}")
    println(s"Finish trigger reports ${myFinishTrigger.getRecordCount()} records were processed");
    println(s"Finish trigger log message is ${myFinishTrigger.getLogMessage()}");

    // Ok, so what were the last 10 states
    println(s"Last parser state was ${parser.getParserState()}")
    println("Previous 10 states were")
    println(parser.getStateTrace.takeRight(10))

    // What was the last WARCInfo record we were working on
    println("Last WARC-Record-ID was: ")
    println(parser.getLastWARCRecord.get("WARC-Record-ID"))
    println(s"Last WARC Record is complete == ${parser.getLastWARCRecord.isComplete()}")
    println(s"Processed ${parser.getRecordCount()}")
    


  } // end main

} // end object
