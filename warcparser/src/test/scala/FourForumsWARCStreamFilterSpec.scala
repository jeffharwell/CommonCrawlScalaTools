package scala

import com.jeffharwell.commoncrawl.warcparser.{FourForumsWARCStreamFilter, WARCConversion, WARCInfo}
import org.scalatest._

class FourForumsWARCStreamFilterSpec extends FlatSpec {

  // Utility class to create a dummy WARCConversion record out of our content string
  // so that we can pass it to the filter for testing.
  def createDummyWARCRecordWithContent(content: String): WARCConversion = {
    // Create the dummy WARCInfo object first
    val warcinforequired = Map[String, String](
      "WARC-Type" -> "warcinfo"
      , "WARC-Date" -> "2016-12-13T03:22:59Z"
      , "WARC-Filename" -> "CC-MAIN-20161202170900-00009-ip-10-31-129-80.ec2.internal.warc.wet.gz"
      , "WARC-Record-ID" -> "<urn:uuid:519aac89-8012-4390-8be6-2d81979f88cb>"
      , "Content-Type" -> "application/warc-fields"
      , "Content-Length" -> "This is my content".getBytes("UTF-8").size.toString()
    )

    val winfo = new WARCInfo()
    winfo.addFields(warcinforequired)
    winfo.addContent("This is my content")

    // Now create the WARCConversion object
    val requiredfields: Map[String, String] = Map[String, String](
      "WARC-Type" -> "conversion",
      "WARC-Target-URI" -> "https://not.a.real.site.com/",
      "WARC-Date" -> "2016-12-13T03:22:59Z",
      "WARC-Record-ID" -> "<urn:uuid:519aac89-8012-4390-8be6-2d81979f88cb>",
      "WARC-Refers-To" -> "DummyRefersToContent",
      "WARC-Block-Digest" -> "DummyDigest",
      "Content-Type" -> "text"
    )

    val w = WARCConversion()
    w.addWARCInfo(winfo)
    w.addFields(requiredfields)
    w.addFields(Map[String, String]("Content-Length" -> content.getBytes("UTF-8").size.toString()))
    w.addContent(content)
    return w
  }

  /*
 * Unit Tests
 */

  "FourForumsWARCStreamFilter" should "accept a paragraph that only mentions the keyword once with minmentions = 1" in {
    val crtest1 = createDummyWARCRecordWithContent(testcontent1)
    val f = new FourForumsWARCStreamFilter()
    f.setMinMentions(1)

    assert(f(crtest1))
  }

  "FourForumsWARCStreamFilter" should "reject a paragraph that only mentions the keyword once with minmentions = 2" in {
    val crtest1 = createDummyWARCRecordWithContent(testcontent1)
    val f = new FourForumsWARCStreamFilter()
    f.setMinMentions(2)

    assert(!f(crtest1))
  }

  "FourForumsWARCStreamFilter" should "reject text when the keyword only appears in its own line" in {
    val crtest1 = createDummyWARCRecordWithContent(taglist)
    val f = new FourForumsWARCStreamFilter()
    f.setMinMentions(1)

    assert(!f(crtest1))
  }

  def testcontent1 =
    """
This is a sentence that mentions guns in a string more than 7 words long. But it only mentions it once.
"""

  def taglist =
    """
gun
abortion
evolution
god
abortion
"""
}