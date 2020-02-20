import collection.mutable.Stack
import scala.collection.immutable.Map
import org.scalatest._
import com.jeffharwell.commoncrawl.warcparser.Reader
import java.io.BufferedInputStream
import java.io.FileInputStream
import java.io.File
import java.io.InputStream

class ReaderSpec extends FlatSpec {

  /*
   * Testing the WET Archive Reader.
   *
   * The key capability needed by the archive reader is the ability to shift between line by line
   * parsing and byte by byte (not character by character!!) parsing. This is strange enough that 
   * I went ahead and implemented this reader.
   *
   * Most tests are duplicated to run once with the default sized internal buffer (1024 bytes) and 
   * once with a very small buffer (2 bytes). This ensures good test coverage as the code in general
   * has two branches, one to handle the case where the next byte or next line is in the internal 
   * buffer already, and another to deal with hitting the end of the internal buffer and needing 
   * to refill it before continuing on. A buffer size of 2 ensures multiple refill operations and 
   * gives me some confidence that the internal buffer handling logic is correct.
   */

  val elevenbytefile = this.getClass().getClassLoader().getResource("11_bytes.txt.gz")
  val sixlinesfile = this.getClass().getClassLoader().getResource("6_line_test.txt.gz")

  /*
   * Testing Reader internal buffer setting
   */
  "reader" should "return size of internal read buffer set by constructor" in {
    val reader = new Reader(new BufferedInputStream(
      new FileInputStream(new File(sixlinesfile.getFile()))), 15)

    assert(reader.getInternalBufferSize() == 15)
  }

  /*
   * Testing interactions between .getLine() and .getStringFromBytes(n)
   */

  "reader" should "return a line from where byte reading left off" in {
    val reader = new Reader(new BufferedInputStream(
      new FileInputStream(new File(sixlinesfile.getFile()))))

    val from_bytes = reader.getStringFromBytes(5)
    val from_line = reader.getLine()

    assert(from_bytes == "hello")
    assert(from_line == " world")
  }

  "reader" should "return a line from where byte reading left off with small buffer" in {
    val reader = new Reader(new BufferedInputStream(
      new FileInputStream(new File(sixlinesfile.getFile()))), 2)

    val from_bytes = reader.getStringFromBytes(5)
    val from_line = reader.getLine()

    assert(from_bytes == "hello")
    assert(from_line == " world")
  }

  "reader" should "return a line from where byte reading left off even on an internal buffer boundry" in {
    val reader = new Reader(new BufferedInputStream(
      new FileInputStream(new File(sixlinesfile.getFile()))), 5)

    val from_bytes = reader.getStringFromBytes(5)
    val from_line = reader.getLine()

    assert(from_bytes == "hello")
    assert(from_line == " world")
  }


  /*
   * Testing Reader.getLine()
   */

  "reader" should "return a line with no line termination characters" in {
    val reader = new Reader(new BufferedInputStream(
      new FileInputStream(new File(sixlinesfile.getFile()))))

    val s = reader.getLine()
    assert(s == "hello world")
  }

  "reader" should "return a line with no line termination characters with small internal buffer" in {
    val reader = new Reader(new BufferedInputStream(
      new FileInputStream(new File(sixlinesfile.getFile()))), 2)

    val s = reader.getLine()
    assert(s == "hello world")
  }



  "reader" should "return next line when called twice" in {
    val reader = new Reader(new BufferedInputStream(
      new FileInputStream(new File(sixlinesfile.getFile()))))

    var s = reader.getLine()
    s = reader.getLine()
    assert(s == "this is a line test")
  }

  "reader" should "return next line when called twice with small internal buffer" in {
    val reader = new Reader(new BufferedInputStream(
      new FileInputStream(new File(sixlinesfile.getFile()))), 2)

    var s = reader.getLine()
    s = reader.getLine()
    assert(s == "this is a line test")
  }



  "reader" should "return an empty string when parsing a blank line" in {
    val reader = new Reader(new BufferedInputStream(
      new FileInputStream(new File(sixlinesfile.getFile()))))

    // throw away the first two to get to the blank line in our
    // test file (bit hacky in here)
    for (i <- 0 to 1) { reader.getLine() }

    var s = reader.getLine()
    assert(s == "")
  }

  "reader" should "return an empty string when parsing a blank line with small internal buffer" in {
    val reader = new Reader(new BufferedInputStream(
      new FileInputStream(new File(sixlinesfile.getFile()))), 2)

    // throw away the first two to get to the blank line in our
    // test file (bit hacky in here)
    for (i <- 0 to 1) { reader.getLine() }

    var s = reader.getLine()
    assert(s == "")
  }



  "reader" should "properly trim windows /r/n style line delimiters" in {
    val reader = new Reader(new BufferedInputStream(
      new FileInputStream(new File(sixlinesfile.getFile()))))

    // throw away the first few to get to the test line in our
    // test file (bit hacky in here)
    for (i <- 0 to 3) { reader.getLine() }

    var s = reader.getLine()
    assert(s == "this line has a \\r\\n line terminator")
  }

  "reader" should "properly trim windows /r/n style line delimiters with small internal buffer" in {
    val reader = new Reader(new BufferedInputStream(
      new FileInputStream(new File(sixlinesfile.getFile()))), 2)

    // throw away the first few to get to the test line in our
    // test file (bit hacky in here)
    for (i <- 0 to 3) { reader.getLine() }

    var s = reader.getLine()
    assert(s == "this line has a \\r\\n line terminator")
  }



  "reader" should "return what it has if end of stream occurns before the newline" in {
    val reader = new Reader(new BufferedInputStream(
      new FileInputStream(new File(sixlinesfile.getFile()))))

    // throw away the first few to get to the test line in our
    // test file (bit hacky in here)
    for (i <- 0 to 4) { reader.getLine() }

    var s = reader.getLine()
    assert(s == "just another line with no EoL")
  }

  "reader" should "return what it has if end of stream occurs before the newline with small internal buffer" in {
    val reader = new Reader(new BufferedInputStream(
      new FileInputStream(new File(sixlinesfile.getFile()))), 2)

    // throw away the first few to get to the test line in our
    // test file (bit hacky in here)
    for (i <- 0 to 4) { reader.getLine() }

    var s = reader.getLine()
    assert(s == "just another line with no EoL")
  }

   
  /*
   * Testing Reader.getStreamFromBytes()
   */

  "reader" should "return return a string of X bytes if available" in {
    val reader = new Reader(new BufferedInputStream(
      new FileInputStream(new File(elevenbytefile.getFile()))))

    val s = reader.getStringFromBytes(5)

    assert(s == "hello")
  }

  "reader" should "return return a string of X bytes if available with small internal buffer" in {
    val reader = new Reader(new BufferedInputStream(
      new FileInputStream(new File(elevenbytefile.getFile()))), 2)

    val s = reader.getStringFromBytes(5)

    assert(s == "hello")
  }



  "reader" should "return all available bytes even if stream ends" in {
    val reader = new Reader(new BufferedInputStream(
      new FileInputStream(new File(elevenbytefile.getFile()))))

    val s = reader.getStringFromBytes(15)

    assert(s == "helloworld\n")
  }

  "reader" should "return all available bytes even if stream ends with small buffer size" in {
    val reader = new Reader(new BufferedInputStream(
      new FileInputStream(new File(elevenbytefile.getFile()))), 2)

    val s = reader.getStringFromBytes(15)

    assert(s == "helloworld\n")
  }



  "reader" should "return number of bytes read if stream ends" in {
    val reader = new Reader(new BufferedInputStream(
      new FileInputStream(new File(elevenbytefile.getFile()))))

    val s = reader.getStringFromBytes(25)

    assert(reader.getBytesRead() == 11)
  }

  "reader" should "return number of bytes read if stream ends with small buffer size" in {
    val reader = new Reader(new BufferedInputStream(
      new FileInputStream(new File(elevenbytefile.getFile()))))

    val s = reader.getStringFromBytes(25)

    assert(reader.getBytesRead() == 11)
  }



  "reader" should "not skip any bytes when called consecutively" in {
    val reader = new Reader(new BufferedInputStream(
      new FileInputStream(new File(elevenbytefile.getFile()))))

    var s = reader.getStringFromBytes(2)
    assert(reader.getBytesRead() == 2)
    assert(s == "he")
    s = reader.getStringFromBytes(3)
    assert(reader.getBytesRead() == 3)
    assert(s == "llo")
    s = reader.getStringFromBytes(10)
    assert(reader.getBytesRead() == 6)
    assert(s == "world\n")
  }

  "reader" should "not skip any bytes when called consecutively with small buffer size" in {
    val reader = new Reader(new BufferedInputStream(
      new FileInputStream(new File(elevenbytefile.getFile()))),2)

    var s = reader.getStringFromBytes(2)
    assert(reader.getBytesRead() == 2)
    assert(s == "he")
    s = reader.getStringFromBytes(3)
    assert(reader.getBytesRead() == 3)
    assert(s == "llo")
    s = reader.getStringFromBytes(10)
    assert(reader.getBytesRead() == 6)
    assert(s == "world\n")

  }



  "reader" should "return false for isEndOfStream() if stream is still open" in {
    val reader = new Reader(new BufferedInputStream(
      new FileInputStream(new File(elevenbytefile.getFile()))))

    reader.getStringFromBytes(2)
    assert(!reader.isEndOfStream())

    reader.getStringFromBytes(3)
    assert(!reader.isEndOfStream())
  }

  "reader" should "return false for isEndOfStream() if stream is still open with small buffer size" in {
    val reader = new Reader(new BufferedInputStream(
      new FileInputStream(new File(elevenbytefile.getFile()))), 2)

    reader.getStringFromBytes(2)
    assert(!reader.isEndOfStream())

    reader.getStringFromBytes(3)
    assert(!reader.isEndOfStream())
  }



  "reader" should "return true for isEndOfStream() if stream closed" in {
    val reader = new Reader(new BufferedInputStream(
      new FileInputStream(new File(elevenbytefile.getFile()))))

    reader.getStringFromBytes(25)
    assert(reader.isEndOfStream())
  }

  "reader" should "throw a RuntimeException if call to getStringFromBytes once stream is closed" in {
    val reader = new Reader(new BufferedInputStream(
      new FileInputStream(new File(elevenbytefile.getFile()))))

    val s = reader.getStringFromBytes(25)

    assert(reader.isEndOfStream())

    assertThrows[RuntimeException] {
      reader.getStringFromBytes(1)
    }

  }

  "reader" should "reply with 0 getBytesRead when asked to read right at end of stream" in {
    val reader = new Reader(new BufferedInputStream(
      new FileInputStream(new File(elevenbytefile.getFile()))))

    val s = reader.getStringFromBytes(11)
    assert(!reader.isEndOfStream())

    // At this point we are at the very end of the input stream, the code just doesn't
    // know it yet. The next read request will return 0 bytes from the Inputstream and result
    // in the underlying reader closing the stream (GZIPInputStream.read(buffer) returns -1)
    reader.getStringFromBytes(1)
    assert(reader.getBytesRead() == 0)
    assert(reader.isEndOfStream())
  }

  "reader" should "output memory debug messages if the debug memory flag is set" in {
    val reader = new Reader(new BufferedInputStream(
      new FileInputStream(new File(sixlinesfile.getFile()))), 5)
    reader.setDebugMemory()

    // https://stackoverflow.com/questions/7218400/scalatest-how-to-test-println
    val stream = new java.io.ByteArrayOutputStream()

    try {
      Console.withOut(stream) {
        val from_bytes = reader.getStringFromBytes(5)
      }
    } catch {
      // no worries, we only care about the debug output
      case e: Exception => true
    }


    val from_line = reader.getLine()

    var output = stream.toString("UTF-8")
    // If you want to see the debug output uncomment the below
    // and then recompile and run the test suite.
    //println(output)
    assert(output.length > 0) // there should be output
    assert(output contains "Heap Size") // this is part of the text the message should contain in the
                                        // case of a memory debug (sloppy I know but gets the job done.)
  }


}
