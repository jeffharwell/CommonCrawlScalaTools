package com.jeffharwell.commoncrawl.warcparser.examples

/* Common Crawl Classes */
import com.jeffharwell.commoncrawl.warcparser.Parser
import com.jeffharwell.commoncrawl.warcparser.WARCRecord
import com.jeffharwell.commoncrawl.warcparser.MyWARCFilter
import com.jeffharwell.commoncrawl.warcparser.MyWARCTopicFilter
import com.jeffharwell.commoncrawl.warcparser.WARCCategorizer
import com.jeffharwell.commoncrawl.warcparser.EmptyCategorizer
import com.jeffharwell.commoncrawl.warcparser.WARCConversion
import com.jeffharwell.commoncrawl.warcparser.ParserTrigger

/* Other Classes */
import scala.collection.mutable.ListBuffer
import java.io.BufferedInputStream
import java.io.InputStream
import java.io.FileInputStream
import java.io.File
import java.net.URL

// Our Default FinishTrigger for testing
class MyParserTrigger extends ParserTrigger {
  var c: Integer = 0
  var recordcount: Integer = 0
  var fileid: Option[String] = None : Option[String]
  var logmessage: Option[String] = None: Option[String]

  override def call(extractedfilename: Option[String], parserrecordcount: Integer, parserlogmessage: Option[String]): Unit = {
    fileid = extractedfilename
    recordcount = parserrecordcount
    logmessage = parserlogmessage
    c += 1
  }
  def getTimesCalled(): Integer = {
    c
  }
  def getFileId(): Option[String] = {
    fileid
  }
  def getRecordCount(): Integer = {
    recordcount
  }
  def getLogMessage(): Option[String] = {
    logmessage
  }
}

object ParserTest {
  def apply(inputstream: InputStream) = {
    new ParserTest(inputstream, new EmptyCategorizer, 0)
  }
  def apply[A <: WARCCategorizer](inputstream: InputStream, categorizer: A) = {
    new ParserTest(inputstream, categorizer, 0)
  }
  def apply(inputstream: InputStream, stepLimit: Int) = {
    new ParserTest(inputstream, new EmptyCategorizer, stepLimit)
  }

}

class ParserTest[A <: WARCCategorizer](inputstream: InputStream, categorizer: A, steplimit: Int) extends Parser(inputstream, categorizer, steplimit) {
  /*
   * This class extends Parser and adds a few methods to inspect the internal state
   * so that we can write more specific test
   */

  /*
   * In the Parser class this method encloses fsa.run() in a while loop that doesn't
   * stop until the FSA hits a sink state or an error state. We want to override that to
   * only call run once so that we can inspect the object after initialization
   * and then step through the FSA manually to test each transition.
   *
   * Just for reference privateNext is the method that is called by the .next() method of
   * the Parser iterator. It is also called on initialization as we want to pre-load the 
   * first record from the input stream so that we can return the right answer when .hasNext()
   * is called.
   */
  //override def privateNext(): Unit = {
  //  var state: State = fsa.run()
  //}

  /* A convenience method so that the unit test code is easier to read (privateNext() doesn't
   * make any sense unless you read the Parser.scala source)
   */
  var lastwarcrecord: WARCConversion = WARCConversion(categorizer)

  def nextFSAStep(): Unit = {
    lastwarcrecord = currentwarcconversion
    privateNext()
  }

  /*
   * This method allows us to look at the internal state of the FSA during testing
   */
  def getParserState(): String = {
    return fsa.getState()
  }

  /*
   * This method inspects the internal state of the Fsa object and returns the 
   * number of bytes of content it expects to read
   */
  def getBytesToRead(): Int = {
    return fsa.bytestoread
  }

  /*
   * Get Return the statetrace 
   */
  def getCurrentWarcConversion(): WARCConversion = {
    return currentwarcconversion
  }

  /*
   * Return the number of steps the internal FSA has taken
   */
  def getSteps(): Int = {
    steps
  }

  /*
   * Get the State Trace
   */
  def getStateTrace(): ListBuffer[State] = {
    statetrace
  }

  /*
   * Get the last WARCConversion Record that we saw
   */
  def getLastWARCRecord(): WARCConversion = {
    lastwarcrecord
  }
}


object TestParserOnArchive {
  def main(args: Array[String]): Unit = {

    println("Reading and filtering a compressed WET archive straight from the web.")
    // This is the path from the wet.paths file
    //var filepath = "crawl-data/CC-MAIN-2016-50/segments/1480698540409.8/wet/CC-MAIN-20161202170900-00007-ip-10-31-129-80.ec2.internal.warc.wet.gz"
    //var filepath = "crawl-data/CC-MAIN-2016-50/segments/1480698542657.90/wet/CC-MAIN-20161202170902-00359-ip-10-31-129-80.ec2.internal.warc.wet.gz"
    var filepath = "crawl-data/CC-MAIN-2016-50/segments/1480698541140.30/wet/CC-MAIN-20161202170901-00007-ip-10-31-129-80.ec2.internal.warc.wet.gz"
    val urlbase = "https://commoncrawl.s3.amazonaws.com/"
    var url = new URL(urlbase+filepath)

    // Create the Filter
    val myfilter = new MyWARCFilter()

    // Create our finish trigger
    val myFinishTrigger = new MyParserTrigger

    // Create the parser
    var urlconnection = url.openConnection()
    urlconnection.setReadTimeout(60000)
    urlconnection.setConnectTimeout(60000)
    var parser = ParserTest(new BufferedInputStream(urlconnection.getInputStream()),1000000)
    parser.setDebug()
	parser.addFinishTrigger(myFinishTrigger)

    val loopcheck = 1000000 // use this to prevent infinite loops
    var looptries = 0 // this is the counter

	// run the parser to state 6, at this point we have found the corrupt record and 
    // are scanning for any new WARC conversion records that we can parse
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
