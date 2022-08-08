import org.scalatest._
import com.jeffharwell.commoncrawl.warcparser.Parser
import com.jeffharwell.commoncrawl.warcparser.WARCRecord
import com.jeffharwell.commoncrawl.warcparser.WARCInfo
import com.jeffharwell.commoncrawl.warcparser.WARCConversion
import com.jeffharwell.commoncrawl.warcparser.EmptyTopicFilter
import com.jeffharwell.commoncrawl.warcparser.MyWARCTopicFilter
import com.jeffharwell.commoncrawl.warcparser.WARCTopicFilter
import com.jeffharwell.commoncrawl.warcparser.ParserTrigger
import com.jeffharwell.commoncrawl.warcparser.ParserTooSlowException
import collection.mutable.Stack
import scala.collection.immutable.Map
import scala.collection.mutable.ListBuffer
import java.io.BufferedInputStream
import java.io.FileInputStream
import java.io.File
import java.io.InputStream

object ParserTest {
  def apply(inputstream: InputStream) = {
    new ParserTest(inputstream, new EmptyTopicFilter, 0)
  }
  def apply[A <: WARCTopicFilter](inputstream: InputStream, categorizer: A) = {
    new ParserTest(inputstream, categorizer, 0)
  }
}

class ParserTest[A <: WARCTopicFilter](inputstream: InputStream, categorizer: A, steplimit: Int) extends Parser(inputstream, categorizer, steplimit) {
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
  override def privateNext(): Unit = {
    var state: State = fsa.run()
  }

  /* A convenience method so that the unit test code is easier to read (privateNext() doesn't
   * make any sense unless you read the Parser.scala source)
   */
  def nextFSAStep(): Unit = {
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
}

object ParserTestLite {
  def apply(inputstream: InputStream) = {
    new ParserTestLite(inputstream, new EmptyTopicFilter, 0)
  }
  def apply(inputstream: InputStream, steplimit: Int) = {
    new ParserTestLite(inputstream, new EmptyTopicFilter, steplimit)
  }

}

class ParserTestLite(inputstream: InputStream, categorizer: EmptyTopicFilter, steplimit: Int) extends Parser(inputstream, categorizer, steplimit) {
  /*
   * This class extends Parser and add a few methods to inspect the internal state
   * so that we can write more specific test
   */

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
   * Returns the size of the internal queue being used to record the elapsed time for the 
   * last n documents that were parsed.
   */
  def getQueueSize(): Int = {
    rate_queue.size
  }

}

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

class ParserSpec extends FlatSpec {

  val frag1 = this.getClass().getClassLoader().getResource("fragment1_incomplete_last.wet.gz")
  val frag2 = this.getClass().getClassLoader().getResource("fragment2_no_header.wet.gz")
  val frag3 = this.getClass().getClassLoader().getResource("fragment3.wet.gz")
  val frag4 = this.getClass().getClassLoader().getResource("appended_zip_fragment_unexpected_eof.wet.gz")
  val frag1_asthma = this.getClass().getClassLoader().getResource("fragment1_asthma_incomplete_last.wet.gz")
  val filter_test_1 = this.getClass().getClassLoader().getResource("filter_test_1.wet.gz")
  val fileheadersonly = this.getClass().getClassLoader().getResource("file_headers_only.wet.gz")
  val headersonly = this.getClass().getClassLoader().getResource("file_and_wet_headers_only.wet.gz")
  val corruptinfo1 = this.getClass().getClassLoader().getResource("corrupt_warcinfo_1.wet.gz")
  val corruptconversion1 = this.getClass().getClassLoader().getResource("corrupt_warcconversion_1.wet.gz")
  val corruptconversion2 = this.getClass().getClassLoader().getResource("corrupt_warcconversion_2.wet.gz")
  val corruptunexpectedconversion = this.getClass().getClassLoader().getResource("corrupt_starts_with_conversion.wet.gz")
  val corruptunexpectedinfo = this.getClass().getClassLoader().getResource("corrupt_two_warcinfo.wet.gz")


  /*
   * Unit Tests
   */


  /*
   * Start and Finish Trigger Tests
   *
   * The Parser allows you to pass an object that will be called when the Parser starts
   * and when the Parser finishes. These specs test that functionality.
   *
   */

  "parser" should "accept an object to be called when it starts parsing" in {
    val myStartTrigger = new MyParserTrigger

    val parser = Parser(new BufferedInputStream(
      new FileInputStream(new File(filter_test_1.getFile()))), 100000)
    parser.addStartTrigger(myStartTrigger)

    var records = ListBuffer[WARCRecord]()
    parser.foreach((wc: WARCRecord) => records += wc)

    assert(myStartTrigger.getTimesCalled() == 1)
    assert(myStartTrigger.getFileId == Some("CC-MAIN-20161202170900-00009-ip-10-31-129-80.ec2.internal.warc.wet.gz"))
  }

  "parser" should "startTrigger should record 1 record parsed in a valid WET archive file" in {
    val myStartTrigger = new MyParserTrigger

    val parser = Parser(new BufferedInputStream(
      new FileInputStream(new File(filter_test_1.getFile()))), 100000)
    parser.addStartTrigger(myStartTrigger)

    var records = ListBuffer[WARCRecord]()
    parser.foreach((wc: WARCRecord) => records += wc)

    assert(myStartTrigger.getRecordCount == 1)
  }

  "parser" should "not return a filename to the startTrigger if it cannot extract the name of the file, meaning there was no valid WARCInfo in the file" in {
    val parser = Parser(new BufferedInputStream(
      new FileInputStream(new File(corruptunexpectedconversion.getFile()))))

    val myStartTrigger = new MyParserTrigger

    parser.addStartTrigger(myStartTrigger)

    var records = ListBuffer[WARCRecord]()
    parser.foreach((wc: WARCRecord) => records += wc)

    assert(myStartTrigger.getFileId == None)
  }

  "parser" should "return zero record count to the startTrigger if it cannot extract the name of the file, meaning there was no valid WARCInfo in the file" in {
    val parser = Parser(new BufferedInputStream(
      new FileInputStream(new File(corruptunexpectedconversion.getFile()))))

    val myStartTrigger = new MyParserTrigger

    parser.addStartTrigger(myStartTrigger)

    var records = ListBuffer[WARCRecord]()
    parser.foreach((wc: WARCRecord) => records += wc)

    assert(myStartTrigger.getRecordCount == 0)
  }

  "parser" should "return a log message to the startTrigger if it cannot extract the name of the file, meaning there was no valid WARCInfo in the file" in {
    val parser = Parser(new BufferedInputStream(
      new FileInputStream(new File(corruptunexpectedconversion.getFile()))))

    val myStartTrigger = new MyParserTrigger

    parser.addStartTrigger(myStartTrigger)

    var records = ListBuffer[WARCRecord]()
    parser.foreach((wc: WARCRecord) => records += wc)

    assert(myStartTrigger.getLogMessage == Some("Unable to Parse File - didn't even get a WARCInfo record. Is the file corrupt?"))
  }

  "parser" should "accept an object to be called when it finishes parsing" in {
    val myFinishTrigger = new MyParserTrigger

    val parser = Parser(new BufferedInputStream(
      new FileInputStream(new File(filter_test_1.getFile()))), 100000)
    parser.addFinishTrigger(myFinishTrigger)

    var records = ListBuffer[WARCRecord]()
    parser.foreach((wc: WARCRecord) => records += wc)

    assert(myFinishTrigger.getTimesCalled() == 1)
  }

  "parser" should "pass the extracted filename to the finishTrigger when it finishes parsing" in {
    val myFinishTrigger = new MyParserTrigger

    val parser = Parser(new BufferedInputStream(
      new FileInputStream(new File(filter_test_1.getFile()))), 100000)
    parser.addFinishTrigger(myFinishTrigger)

    var records = ListBuffer[WARCRecord]()
    parser.foreach((wc: WARCRecord) => records += wc)

    assert(myFinishTrigger.getFileId == Some("CC-MAIN-20161202170900-00009-ip-10-31-129-80.ec2.internal.warc.wet.gz"))
  }

  "parser" should "pass the record count to the finishTrigger when it finishes parsing and it must match the number of records that are parsed" in {
    val myFinishTrigger = new MyParserTrigger

    val parser = Parser(new BufferedInputStream(
      new FileInputStream(new File(filter_test_1.getFile()))), 100000)
    parser.addFinishTrigger(myFinishTrigger)

    var records = ListBuffer[WARCRecord]()
    parser.foreach((wc: WARCRecord) => records += wc)

    assert(myFinishTrigger.getRecordCount == records.size)
  }

  "parser" should "give a success log message to the finishTrigger if parsing is successful and records are retreived" in {
    val myFinishTrigger = new MyParserTrigger

    val parser = Parser(new BufferedInputStream(
      new FileInputStream(new File(filter_test_1.getFile()))), 100000)
    parser.addFinishTrigger(myFinishTrigger)

    var records = ListBuffer[WARCRecord]()
    parser.foreach((wc: WARCRecord) => records += wc)

    assert(myFinishTrigger.getLogMessage == Some("File Parsed Normally"))
  }

  "parser" should "give a confused log message to the finishTrigger if the finished trigger is added after parsing is completed" in {
    val myFinishTrigger = new MyParserTrigger

    val parser = Parser(new BufferedInputStream(
      new FileInputStream(new File(filter_test_1.getFile()))), 100000)

    var records = ListBuffer[WARCRecord]()
    parser.foreach((wc: WARCRecord) => records += wc)

    parser.addFinishTrigger(myFinishTrigger)

    assert(myFinishTrigger.getLogMessage == Some("File Parsed Normally and retrieved records ... did you added the trigger after the parse was already done??"))
  }


  "parser" should "not return an extracted file name to the finishTrigger if it cannot extract the name of the file, meaning there was no valid WARCInfo in the file" in {
    val parser = Parser(new BufferedInputStream(
      new FileInputStream(new File(corruptunexpectedconversion.getFile()))))

    val myFinishTrigger = new MyParserTrigger

    parser.addFinishTrigger(myFinishTrigger)

    var records = ListBuffer[WARCRecord]()
    parser.foreach((wc: WARCRecord) => records += wc)

    assert(myFinishTrigger.getFileId == None)
  }

  "parser" should "return a no wARC Info log message to the finishTrigger if it cannot extract the name of the file, meaning there was no valid WARCInfo in the file" in {
    val parser = Parser(new BufferedInputStream(
      new FileInputStream(new File(corruptunexpectedconversion.getFile()))))

    val myFinishTrigger = new MyParserTrigger

    parser.addFinishTrigger(myFinishTrigger)

    var records = ListBuffer[WARCRecord]()
    parser.foreach((wc: WARCRecord) => records += wc)

    assert(myFinishTrigger.getLogMessage == Some("File Parse Error - Corruption detected, was not able to find complete WARCInfo record"))
  }

  "parser" should "return a no WARC Conversion records log message if the file parses but no WARC Conversion records are found" in {
    val parser = Parser(new BufferedInputStream(
      new FileInputStream(new File(corruptconversion2.getFile()))))

    val myFinishTrigger = new MyParserTrigger

    parser.addFinishTrigger(myFinishTrigger)

    var records = ListBuffer[WARCRecord]()
    parser.foreach((wc: WARCRecord) => records += wc)

    assert(records.size == 0)
    assert(parser.getRecordCount() == 0)
    assert(myFinishTrigger.getLogMessage == Some("File Parse Error - Corruption detected and no WARC Conversion records were extracted"))
  }

  "parser" should "return a corruption detected log message after finding 4 WARCRecord objects and skipping a corrupt record in file corruptconversion1" in {
    val parser = Parser(new BufferedInputStream(
      new FileInputStream(new File(corruptconversion1.getFile()))))

    val myFinishTrigger = new MyParserTrigger
    parser.addFinishTrigger(myFinishTrigger)

    var records = ListBuffer[WARCRecord]()

    parser.foreach((wc: WARCRecord) => records += wc)

    assert(myFinishTrigger.getLogMessage == Some("File Parsed - Some corruption detected"))
  }

  /*
   * Parser Rate Tests
   *
   * Test the requirements that the parser measure and report the rate at which it is parsing documents.
   *
   */

  "parser" should "allow us to enable parser rate debug messages" in {
    val parser = Parser(new BufferedInputStream(
      new FileInputStream(new File(filter_test_1.getFile()))), 100000)
    parser.setDebugRate()
  }

  "parser" should "report the average time it took to parse the last 10 documents" in {
    val myFinishTrigger = new MyParserTrigger

    val parser = Parser(new BufferedInputStream(
      new FileInputStream(new File(filter_test_1.getFile()))), 100000)
    var one_hour = 3600000
    parser.setRateLimit(one_hour, 5) // There is something wrong with your computer if you can't hit one record per hour

    var records = ListBuffer[WARCRecord]()
    parser.foreach((wc: WARCRecord) => records += wc)

    assert(parser.getAverageParseRate > 0)
  }

  "parser" should "should not track the elapsed time if no rate limit is set" in {
    val myFinishTrigger = new MyParserTrigger

    val parser = ParserTestLite(new BufferedInputStream(
      new FileInputStream(new File(filter_test_1.getFile()))), 100000)

    var records = ListBuffer[WARCRecord]()
    parser.foreach((wc: WARCRecord) => records += wc)

    // We have 25 documents in the file filter_test_1, so if our default queue_size is 10
    // we should end up with 10 times in the rate queue
    assert(parser.getQueueSize() == 0)
    assert(parser.getAverageParseRate == 0)
  }


  "parser" should "should use a default queue size of 10 when setting a rate limit" in {
    val myFinishTrigger = new MyParserTrigger

    val parser = ParserTestLite(new BufferedInputStream(
      new FileInputStream(new File(filter_test_1.getFile()))), 100000)
    var one_hour = 3600000
    parser.setRateLimit(one_hour, 5) // There is something wrong with your computer if you can't hit one record per hour

    var records = ListBuffer[WARCRecord]()
    parser.foreach((wc: WARCRecord) => records += wc)

    // We have 25 documents in the file filter_test_1, so if our default queue_size is 10
    // we should end up with 10 times in the rate queue
    assert(parser.getQueueSize() == 5)
  }

  "parser" should "should set the queue size to 5 if that argument is passed while setting a rate limit" in {
    val myFinishTrigger = new MyParserTrigger

    val parser = ParserTestLite(new BufferedInputStream(
      new FileInputStream(new File(filter_test_1.getFile()))), 100000)
    var one_hour = 3600000
    parser.setRateLimit(one_hour, 5) // There is something wrong with your computer if you can't hit one record per hour

    var records = ListBuffer[WARCRecord]()
    parser.foreach((wc: WARCRecord) => records += wc)

    // We have 25 documents in the file filter_test_1, so if our default queue_size is 10
    // we should end up with 10 times in the rate queue
    assert(parser.getQueueSize() == 5)
  }

  "parser" should "should have a queue size equal to the number of documents parsed if queue size is greater than the number of documents in the file" in {
    val myFinishTrigger = new MyParserTrigger

    val parser = ParserTestLite(new BufferedInputStream(
      new FileInputStream(new File(filter_test_1.getFile()))), 100000)
    var one_hour = 3600000
    parser.setRateLimit(one_hour, 50) // There is something wrong with your computer if you can't hit one record per hour

    var records = ListBuffer[WARCRecord]()
    parser.foreach((wc: WARCRecord) => records += wc)

    assert(records.size < 50) // records.size for filter_test_1 will be 25
    assert(parser.getQueueSize() == records.size)
  }

  "parser" should "throw a ParserTooSlowException if the parse rate is set very low" in {
    val myFinishTrigger = new MyParserTrigger

    val parser = Parser(new BufferedInputStream(
      new FileInputStream(new File(filter_test_1.getFile()))), 100000)
    parser.setRateLimit(.0000001) // it should take Moore's law a bit to catch up to this. Parsing a record per nanosecond.

    var records = ListBuffer[WARCRecord]()

    assertThrows[ParserTooSlowException] {
      parser.foreach((wc: WARCRecord) => records += wc)
    }

    assert(parser.getAverageParseRate > 0)
  }

  "parser" should "output debug messages if the parse rate is set very low and debug is set" in {
    val myFinishTrigger = new MyParserTrigger

    // https://stackoverflow.com/questions/7218400/scalatest-how-to-test-println
    val stream = new java.io.ByteArrayOutputStream()

    val parser = Parser(new BufferedInputStream(
      new FileInputStream(new File(filter_test_1.getFile()))), 100000)
    parser.setRateLimit(.0000001) // it should take Moore's law a bit to catch up to this. Parsing a record per nanosecond.
    parser.setDebugRate()

    var records = ListBuffer[WARCRecord]()

    try {
      Console.withOut(stream) {
        parser.foreach((wc: WARCRecord) => records += wc)
      }
      // if you care to see the debug messages, uncomment the below and
      // run the test suite.
    } catch {
      // no worries, we were expecting an exception, we only care about the debug output
      case e: Exception => true
    }
    var output = stream.toString("UTF-8")
    // If you want to see the debug output uncomment the below
    // and then recompile and run the test suite.
    //println(output)
    assert(output.length > 0) // there should be output
    assert(output contains "The current parse cycle has taken") // this is part of the text the message should contain in the
                                                               // case of a slow parse (a bit sloppy I know but gets the job done.)
  }

  "parser" should "not throw a ParserTooSlowException if the parse rate is very high" in {
    val myFinishTrigger = new MyParserTrigger

    val parser = Parser(new BufferedInputStream(
      new FileInputStream(new File(filter_test_1.getFile()))), 100000)

    var one_hour = 3600000
    parser.setRateLimit(one_hour) // There is something wrong with your computer if you can't hit one record per hour

    var records = ListBuffer[WARCRecord]()

    parser.foreach((wc: WARCRecord) => records += wc)

    assert(parser.getAverageParseRate < one_hour)
  }

  "parser" should "should not throw a ParserTooSlowException/test the rate until the queue is full" in {
    val myFinishTrigger = new MyParserTrigger

    val parser = ParserTestLite(new BufferedInputStream(
      new FileInputStream(new File(filter_test_1.getFile()))), 100000)
    var one_nanosecond = .0000001 // it should take Moore's law a bit to catch up to this. Parsing a record per nanosecond.
    parser.setRateLimit(one_nanosecond, 50) 

    var records = ListBuffer[WARCRecord]()
    parser.foreach((wc: WARCRecord) => records += wc)

    assert(records.size < 50) // records.size for filter_test_1 will be 25
    assert(parser.getQueueSize() == records.size)
  }

  /*
   * Record Count Tests
   *
   * The parser should make the number of records parsed available
   *
   */

  "parser" should "have a record count of one upon initial transition to state S5" in
  {
    val parser = ParserTest(new BufferedInputStream(
      new FileInputStream(new File(frag3.getFile()))))
    parser.setDebug()
    val stopstates = List[String]("S5","Sink1","Final")

    while (!stopstates.contains(parser.getParserState())) {
      parser.nextFSAStep()
    }

    assert(parser.getParserState == "S5")
    assert(parser.getRecordCount() == 1)
  }

  "parser" should "have a record count of 5 after extracting 5 WARC conversion records from fragment3.wet.gz" in
  {
    // Which in turn should match the number of records it actually returns, 5
    val parser = Parser(new BufferedInputStream(
      new FileInputStream(new File(frag3.getFile()))))

    parser.setDebug()
    var records = ListBuffer[WARCRecord]()
    parser.foreach((wc: WARCRecord) => records += wc)

    assert(records.size == 5)
    assert(parser.getRecordCount() == 5)
  }

  "parser" should "have a record count of 0 if there are no valid WARC conversion records in a file" in {
    val parser = Parser(new BufferedInputStream(
      new FileInputStream(new File(corruptunexpectedconversion.getFile()))))

    var records = ListBuffer[WARCRecord]()
    parser.foreach((wc: WARCRecord) => records += wc)

    assert(records.size == 0)
    assert(parser.getRecordCount() == 0)
  }

  "parser" should "set the memoryDebug on the reader and output debug messages regarding memory usage if the debug memory flag is set" in {
    val myFinishTrigger = new MyParserTrigger

    // https://stackoverflow.com/questions/7218400/scalatest-how-to-test-println
    val stream = new java.io.ByteArrayOutputStream()

    val parser = Parser(new BufferedInputStream(
      new FileInputStream(new File(filter_test_1.getFile()))), 100000)
    parser.setRateLimit(.0000001) // it should take Moore's law a bit to catch up to this. Parsing a record per nanosecond.
    parser.setDebugMemory()

    var records = ListBuffer[WARCRecord]()

    try {
      Console.withOut(stream) {
        parser.foreach((wc: WARCRecord) => records += wc)
      }
    } catch {
      // no worries, we were expecting an exception, we only care about the debug output
      case e: Exception => true
    }
    var output = stream.toString("UTF-8")
    // If you want to see the debug output uncomment the below
    // and then recompile and run the test suite.
    //println(output)
    assert(output.length > 0) // there should be output
    assert(output contains "Heap Size") // this is part of the text the message should contain in the
                                        // case of a memory debug (sloppy I know but gets the job done.)
  }


  /*
   * Categorizer Tests
   *
   * These test the categorizer function of the Parser.
   *
   */

  "parser" should "accept a categorizer to use when parsing records" in {
    val c: MyWARCTopicFilter = new MyWARCTopicFilter(4)
    val parser = Parser(new BufferedInputStream(
      new FileInputStream(new File(frag1_asthma.getFile()))), c, 1000)
    var records = ListBuffer[WARCRecord]()
    parser.foreach((wc: WARCRecord) => records += wc)
    val cat = records(0).getCategories() match {
      case Some(a) => a
      case _ => Set[String]()
    }
    assert(cat.contains("asthma"))
  }

  "parser" should "apply the categorizer to multiple records" in {
    val c: MyWARCTopicFilter = new MyWARCTopicFilter(1)
    //val parser = Parser(new BufferedInputStream(
    //  new FileInputStream(new File(frag1_asthma.getFile()))), c, 1000)
    val parser = Parser(new BufferedInputStream(
      new FileInputStream(new File(filter_test_1.getFile()))), c, 100000)
    var records = ListBuffer[WARCRecord]()
    parser.foreach((wc: WARCRecord) => records += wc)

	// Now grab all the documents where the content mentions asthma at all
	// using a simple filter and the contains method.
	// Essentially this pulls out all the asthma content, as MyWARCFilter
	// matched both asthma and politics content
	def mycontentfilter(content: Option[String]): Boolean = {
		content match {
			case Some(c) => c.contains("asthma")
			case _ => false
		}
	}
	val l = records.filter(y => mycontentfilter(y.getContent()))

    def getcat(record: WARCRecord) = { 
        record.getCategories() match {
        case Some(a) => a
        case _ => Set[String]()
      }
    }

    l.foreach((lr) => {
      assert(getcat(lr).contains("asthma"))
    })
  }

  /*
   * Parser State Tests
   *
   * These tests are designed to required the parser to be in specific external states upon
   * reading certain input. They test the internal logic of the Parser FSA
   *
   */

  // Testing State 1
  "parser" should "be in state one after initial header read" in 
  {
    val parser = ParserTest(new BufferedInputStream(
      new FileInputStream(new File(frag3.getFile()))))

    assert(parser.getParserState() == "S1")
  }

  "parser" should "be in state two after seven calls to run using frag3" in 
  {
    val parser = ParserTest(new BufferedInputStream(
      new FileInputStream(new File(frag3.getFile()))))

    for (i <- 0 to 6) {
      parser.nextFSAStep()
    }

    assert(parser.getParserState() == "S2")
  }

  "parser" should "have complete WARCInfo headers but no content after seven calls to run with frag3" in 
  {
    val parser = ParserTest(new BufferedInputStream(
      new FileInputStream(new File(frag3.getFile()))))

    for (i <- 0 to 5) {
      parser.nextFSAStep()
    }

    assert(parser.currentwarcinfo.headersComplete() == true)
  }

  "parser" should "know there are 259 bytes of content to read after info headers are complete in frag3" in
  {
    val parser = ParserTest(new BufferedInputStream(
      new FileInputStream(new File(frag3.getFile()))))

    while (parser.getParserState() == "S1") {
      parser.nextFSAStep()
    }

    assert(parser.getBytesToRead() == 259)
  }

  "parser" should "have a complete WARCInfo record after a run call while in State S2 with frag3 file" in
  {
    val parser = ParserTest(new BufferedInputStream(
      new FileInputStream(new File(frag3.getFile()))))

    // This will get the headers and progress to S2
    while (parser.getParserState() != "S2") {
      parser.nextFSAStep()
    }

    assert(parser.getParserState() == "S2")

    parser.nextFSAStep() // this should read in the content

    assert(parser.currentwarcinfo.isComplete() == true)
  }

  "parser" should "move to state S3 after getting a complete WARCInfo record" in
  {
    val parser = ParserTest(new BufferedInputStream(
      new FileInputStream(new File(frag3.getFile()))))

    // This will get the headers and progress to S2
    while (parser.getParserState() == "S1" || parser.getParserState() == "S2" ) {
      parser.nextFSAStep()
    }

    assert(parser.getParserState() == "S3")

    assert(parser.currentwarcinfo.isComplete() == true)
  }

  "parser" should "have complete WARCConversion headers upon transition to state S4" in
  {
    val parser = ParserTest(new BufferedInputStream(
      new FileInputStream(new File(frag3.getFile()))))

    while (parser.getParserState() != "S4" && parser.getParserState() != "Sink1") {
      parser.nextFSAStep()
    }

    assert(parser.getParserState() == "S4")
    assert(parser.currentwarcconversion.headersComplete() == true)
  }

  "parser" should "know there are 2287 bytes of content in first WARCConversion record of frag3" in 
  {
    val parser = ParserTest(new BufferedInputStream(
      new FileInputStream(new File(frag3.getFile()))))

    while (parser.getParserState() != "S4" && parser.getParserState() != "Sink1") {
      parser.nextFSAStep()
    }

    assert(parser.getParserState() == "S4")
    assert(parser.getBytesToRead() == 2287)
  }

  "parser" should "have a complete WARCConversion record upon transition to state S5" in
  {
    val parser = ParserTest(new BufferedInputStream(
      new FileInputStream(new File(frag3.getFile()))))
    parser.setDebug()
    val stopstates = List[String]("S5","Sink1","Final")

    while (!stopstates.contains(parser.getParserState())) {
      parser.nextFSAStep()
    }

    assert(parser.getParserState == "S5")
    assert(parser.currentwarcconversion.isComplete())
  }

  "parser" should "extract 5 WARC conversion records from fragment3.wet.gz" in
  {
    val parser = Parser(new BufferedInputStream(
      new FileInputStream(new File(frag3.getFile()))))
    parser.setDebug()
    var records = ListBuffer[WARCRecord]()

    parser.foreach((wc: WARCRecord) => records += wc)
    assert(records.size == 5)
  }

  "parser" should "end in the Final state after extracting all records from fragment3.wet.gz" in
  {
    val parser = ParserTestLite(new BufferedInputStream(
      new FileInputStream(new File(frag3.getFile()))))
    parser.setDebug()
    var records = ListBuffer[WARCRecord]()

    parser.foreach((wc: WARCRecord) => records += wc)
    assert(parser.getParserState == "Final")
  }

  /*
   * Test for proper handling of file corruption
   */

  "parser" should "bail to error Sink1 if unable to get a complete warcinfo header in a predetermined number of tries" in {
    val parser = ParserTest(new BufferedInputStream(
      new FileInputStream(new File(corruptinfo1.getFile()))))
    parser.setDebug()

    // limit is hard coded as # of required WARC fields + 4
    val winfo = new WARCInfo()
    val limit = winfo.numberRequiredFields() + 4
    var tries = 0

    while (parser.getParserState() == "S1" && tries < limit + 5 ) {
      tries += 1
      parser.nextFSAStep()
    }

    assert(parser.getParserState == "Sink1") // we should end up in the error state
    assert(tries == limit) // we should have tried up to the limit 
  }

  "parser" should "bail to error Sink1 if the initial WARC record is not of type 'warcinfo'" in {
    val parser = ParserTest(new BufferedInputStream(
      new FileInputStream(new File(corruptunexpectedconversion.getFile()))))
    parser.setDebug()

    val winfo = new WARCInfo()
    val limit = 100
    var tries = 0

    while (parser.getParserState() == "S1" && tries < limit ) {
      tries += 1
      parser.nextFSAStep()
    }
    assert(tries < limit) // infinite loop?
    assert(parser.getParserState == "Sink1") // we should end up in the error state
  }

  "parser" should "bail to error Sink1 if unable to get a complete warcinfo header in a predetermined number of tries because no warcinfo record exists" in {
    val parser = ParserTest(new BufferedInputStream(
      new FileInputStream(new File(frag2.getFile()))))
    parser.setDebug()

    // limit is hard coded as # of required WARC fields + 4
    val winfo = new WARCInfo()
    val limit = winfo.numberRequiredFields() + 4
    var tries = 0

    while (parser.getParserState() == "S1" && tries < limit + 5 ) {
      tries += 1
      parser.nextFSAStep()
    }

    assert(parser.getParserState == "Sink1") // we should end up in the error state
    assert(tries == limit) // we should have tried up to the limit 
  }

  "parser" should "move to scan for additional WARC conversion records if unable to get a complete set of WARC conversion headers in a predefined number of tries" in {
    val parser = ParserTest(new BufferedInputStream(
      new FileInputStream(new File(corruptconversion1.getFile()))))
    parser.setDebug()

    // limit is hard coded as # of required WARC fields + 4
    val winfo = WARCConversion()
    val limit = winfo.numberRequiredFields() + 4
    var tries = 0

    // Alright, this is a bit trick
    val loopcheck = 100 // use this to prevent infinite loops
    var looptries = 0 // this is the counter

    // now run the parser through to the transition to S3, this means
    // we already have the WARC info record parsed. For this resource file
    // we know that the first WARC conversion record is corrupt, missing a header
    while (parser.getParserState() != "S3" && looptries < loopcheck ) {
      looptries += 1
      parser.nextFSAStep()
    }
    assert(looptries < loopcheck) // this is our infinite loop check, the parser
                                  // failed to progress to state 3, other test should
                                  // have failed as well, don't start with this one.

    // new we hit state 3, it should try limit times and then proceed to state S6
    looptries = 0
    while (parser.getParserState() == "S3" && looptries < loopcheck) {
      looptries += 1
      tries += 1 // I know .. clarity over brevity
      parser.nextFSAStep()
    }

    assert(parser.getParserState == "S6") // we should end up in the error state
    assert(tries == limit) // we should have tried up to the limit 
  }

  "parser" should "find an additional WARC conversion record if unable to get a complete set of WARC conversion headers in a previous record" in {
    val parser = ParserTest(new BufferedInputStream(
      new FileInputStream(new File(corruptconversion1.getFile()))))
    parser.setDebug()

    // limit is hard coded as # of required WARC fields + 4
    val winfo = WARCConversion()
    val limit = winfo.numberRequiredFields() + 4
    var tries = 0

    // Alright, this is a bit trick
    val loopcheck = 100 // use this to prevent infinite loops
    var looptries = 0 // this is the counter

    // now run the parser through to the transition to S3, this means
    // we already have the WARC info record parsed. For this resource file
    // we know that the first WARC conversion record is corrupt, missing a header
    while (parser.getParserState() != "S3" && looptries < loopcheck ) {
      looptries += 1
      parser.nextFSAStep()
    }
    assert(looptries < loopcheck) // this is our infinite loop check, the parser
                                  // failed to progress to state 3, other test should
                                  // have failed as well, don't start with this one.

    // run the parser to state 6, at this point we should be scanning for any new 
    // WARC conversion records that we can parse
    looptries = 0
    while (parser.getParserState() != "S6" && looptries < loopcheck) {
      looptries += 1
      parser.nextFSAStep()
    }
    assert(looptries < loopcheck) // infinite loop?

    // now run until state 6 finds the new record and passes back to state 3 for parsing
    looptries = 0
    while (parser.getParserState() != "S3" && looptries < loopcheck) {
      looptries += 1
      parser.nextFSAStep()
    }
    assert(looptries < loopcheck) // infinite loop?
    assert(parser.getParserState == "S3") // we should end up in S3, ready to parse new record
  }

  "parser" should "end in an error state (Sink2) if no complete conversion records can be found after a corrupt record is detected" in {
    val parser = ParserTest(new BufferedInputStream(
      new FileInputStream(new File(corruptconversion2.getFile()))))
    parser.setDebug()

    // limit is hard coded as # of required WARC fields + 4
    val winfo = WARCConversion()
    val limit = winfo.numberRequiredFields() + 4
    var tries = 0

    // Alright, this is a bit trick
    val loopcheck = 100 // use this to prevent infinite loops
    var looptries = 0 // this is the counter

    // run the parser to state 6, at this point we have found the corrupt record and 
    // are scanning for any new WARC conversion records that we can parse
    looptries = 0
    while (parser.getParserState() != "S6" && looptries < loopcheck) {
      looptries += 1
      parser.nextFSAStep()
    }
    assert(looptries < loopcheck) // infinite loop?

    // now run until state Sink2
    looptries = 0
    while (parser.getParserState() != "Sink2" && looptries < loopcheck) {
      looptries += 1
      parser.nextFSAStep()
    }
    assert(looptries < loopcheck) // infinite loop?
    assert(parser.getParserState == "Sink2") // we should end up in the error state
  }

  "parser" should "stop when it reaches the error state (Sink2) when no complete conversion records can be found after a corrupt record is detected" in {
    val parser = ParserTestLite(new BufferedInputStream(
      new FileInputStream(new File(corruptconversion2.getFile()))), 1000)
    assert(parser.getParserState == "Sink2")
  }

  "parser" should "skip any non-conversion type records after the 'warcinfo' type record is parsed" in {
    val parser = Parser(new BufferedInputStream(
      new FileInputStream(new File(corruptunexpectedinfo.getFile()))))
    parser.setDebug()
    var records = ListBuffer[WARCRecord]()

    parser.foreach((wc: WARCRecord) => records += wc)
    assert(records.size == 5)

    // Make sure they are all conversion type records
    records.foreach( r => {
      assert(r.fields("WARC-Type") == "conversion")
    })
  }

  "parser" should "return 4 WARCRecord objects skipping corrupt record in file corruptconversion1" in {
    val parser = Parser(new BufferedInputStream(
      new FileInputStream(new File(corruptconversion1.getFile()))))
    parser.setDebug()
    var records = ListBuffer[WARCRecord]()

    parser.foreach((wc: WARCRecord) => records += wc)
    assert(records.size == 4)
  }


  /*
   * I probably also need to extend the WARCRecord classes so that they check to make sure
   * they are actually getting a warcinfo or conversion type record and throw an error if 
   * the Content-Type field isn't what they are expecting
   */

}
