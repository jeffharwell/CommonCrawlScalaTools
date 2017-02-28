import org.scalatest._
import com.jeffharwell.commoncrawl.createcorpus.Parser
import com.jeffharwell.commoncrawl.createcorpus.WARCRecord
import com.jeffharwell.commoncrawl.createcorpus.WARCInfo
import com.jeffharwell.commoncrawl.createcorpus.WARCConversion
import collection.mutable.Stack
import scala.collection.immutable.Map
import scala.collection.mutable.ListBuffer
import java.io.BufferedInputStream
import java.io.FileInputStream
import java.io.File
import java.io.InputStream

class ParserTest(inputstream: InputStream) extends Parser(inputstream) {
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

class ParserTestLite(inputstream: InputStream) extends Parser(inputstream) {
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
}


class ParserSpec extends FlatSpec {

  val frag1 = this.getClass().getClassLoader().getResource("fragment1_incomplete_last.wet.gz")
  val frag2 = this.getClass().getClassLoader().getResource("fragment2_no_header.wet.gz")
  val frag3 = this.getClass().getClassLoader().getResource("fragment3.wet.gz")
  val frag4 = this.getClass().getClassLoader().getResource("appended_zip_fragment_unexpected_eof.wet.gz")
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

  // Testing State 1
  "parser" should "be in state one after initial header read" in 
  {
    val parser = new ParserTest(new BufferedInputStream(
      new FileInputStream(new File(frag3.getFile()))))

    assert(parser.getParserState() == "S1")
  }

  "parser" should "be in state two after seven calls to run using frag3" in 
  {
    val parser = new ParserTest(new BufferedInputStream(
      new FileInputStream(new File(frag3.getFile()))))

    for (i <- 0 to 6) {
      parser.nextFSAStep()
    }

    assert(parser.getParserState() == "S2")
  }

  "parser" should "have complete WARCInfo headers but no content after seven calls to run with frag3" in 
  {
    val parser = new ParserTest(new BufferedInputStream(
      new FileInputStream(new File(frag3.getFile()))))

    for (i <- 0 to 5) {
      parser.nextFSAStep()
    }

    assert(parser.currentwarcinfo.headersComplete() == true)
  }

  "parser" should "know there are 259 bytes of content to read after info headers are complete in frag3" in
  {
    val parser = new ParserTest(new BufferedInputStream(
      new FileInputStream(new File(frag3.getFile()))))

    while (parser.getParserState() == "S1") {
      parser.nextFSAStep()
    }

    assert(parser.getBytesToRead() == 259)
  }

  "parser" should "have a complete WARCInfo record after a run call while in State S2 with frag3 file" in
  {
    val parser = new ParserTest(new BufferedInputStream(
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
    val parser = new ParserTest(new BufferedInputStream(
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
    val parser = new ParserTest(new BufferedInputStream(
      new FileInputStream(new File(frag3.getFile()))))

    while (parser.getParserState() != "S4" && parser.getParserState() != "Sink1") {
      parser.nextFSAStep()
    }

    assert(parser.getParserState() == "S4")
    assert(parser.currentwarcconversion.headersComplete() == true)
  }

  "parser" should "know there are 2287 bytes of content in first WARCConversion record of frag3" in 
  {
    val parser = new ParserTest(new BufferedInputStream(
      new FileInputStream(new File(frag3.getFile()))))

    while (parser.getParserState() != "S4" && parser.getParserState() != "Sink1") {
      parser.nextFSAStep()
    }

    assert(parser.getParserState() == "S4")
    assert(parser.getBytesToRead() == 2287)
  }

  "parser" should "have a complete WARCConversion record upon transition to state S5" in
  {
    val parser = new ParserTest(new BufferedInputStream(
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
    val parser = new Parser(new BufferedInputStream(
      new FileInputStream(new File(frag3.getFile()))))
    parser.setDebug()
    var records = ListBuffer[WARCRecord]()

    parser.foreach((wc: WARCRecord) => records += wc)
    assert(records.size == 5)
  }

  "parser" should "end in the Final state after extracting all records from fragment3.wet.gz" in
  {
    val parser = new ParserTestLite(new BufferedInputStream(
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
    val parser = new ParserTest(new BufferedInputStream(
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
    val parser = new ParserTest(new BufferedInputStream(
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
    val parser = new ParserTest(new BufferedInputStream(
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
    val parser = new ParserTest(new BufferedInputStream(
      new FileInputStream(new File(corruptconversion1.getFile()))))
    parser.setDebug()

    // limit is hard coded as # of required WARC fields + 4
    val winfo = new WARCConversion()
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
    val parser = new ParserTest(new BufferedInputStream(
      new FileInputStream(new File(corruptconversion1.getFile()))))
    parser.setDebug()

    // limit is hard coded as # of required WARC fields + 4
    val winfo = new WARCConversion()
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
    val parser = new ParserTest(new BufferedInputStream(
      new FileInputStream(new File(corruptconversion2.getFile()))))
    parser.setDebug()

    // limit is hard coded as # of required WARC fields + 4
    val winfo = new WARCConversion()
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

  "parser" should "skip any non-conversion type records after the 'warcinfo' type record is parsed" in {
    val parser = new Parser(new BufferedInputStream(
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
    val parser = new Parser(new BufferedInputStream(
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
