import org.scalatest._
import Matchers._
import com.jeffharwell.commoncrawl.createcorpus.WARCRecord
import com.jeffharwell.commoncrawl.createcorpus.WARCInfo
import com.jeffharwell.commoncrawl.createcorpus.WARCRecordTypeException
import com.jeffharwell.commoncrawl.createcorpus.WARCConversion
//import scala.collection.mutable.Map

class WARCConversionSpec extends FlatSpec {

  // Default warcinfo type required fields and values
  val warcinforequired = Map[String,String](
     "WARC-Type" -> "warcinfo"
    ,"WARC-Date" -> "2016-12-13T03:22:59Z"
    ,"WARC-Filename" -> "CC-MAIN-20161202170900-00009-ip-10-31-129-80.ec2.internal.warc.wet.gz"
    ,"WARC-Record-ID" -> "<urn:uuid:519aac89-8012-4390-8be6-2d81979f88cb>"
    ,"Content-Type" -> "application/warc-fields"
    ,"Content-Length" -> "259")

  "WARCInfo Object" should "have these 7 specific required fields" in {
    val requiredfields: List[String] = List[String](
                                                  "WARC-Type",
						  "WARC-Date",
						  "WARC-Filename",
						  "WARC-Record-ID",
						  "Content-Type",
						  "Content-Length",
						  "Content")
    
    val warcinfo = new WARCInfo()
    assert(requiredfields.toSet == warcinfo.requiredfields.toSet)
  }

  "WARCConversion Object" should "have these 9 specific required fields" in {
    val requiredfields: List[String] = List[String](
                                                  "WARC-Type",
                                                  "WARC-Target-URI",
                                                  "WARC-Date",
                                                  "WARC-Record-ID",
                                                  "WARC-Refers-To",
                                                  "WARC-Block-Digest",
                                                  "Content-Type",
                                                  "Content-Length",
                                                  "Content")
    
    val warc = new WARCConversion()
    assert(requiredfields.toSet == warc.requiredfields.toSet)
  }

  "WARCConversion Object" should "report that it has 9 required fields at initialization" in {
    val warc = new WARCConversion()
    assert(warc.numberRequiredFields == 9)
  }

  "WARCConversion" should "throw WARCRecordTypeException if WARC-Type is not 'conversion'" in {
    val w = new WARCConversion()

    assertThrows[WARCRecordTypeException] { w.addFields(Map("WARC-Type" -> "warcinfo")) }
  }

  "WARCConversion" should "be complete with these fields and content" in {
    // First create the WARCInfo we need
    val winfo = new WARCInfo()
    winfo.addFields(warcinforequired)
    winfo.addContent("This is my content") 
 
    val w: WARCConversion = new WARCConversion()

    val requiredfields: Map[String,String] = Map[String,String](
      "WARC-Type" -> "conversion",
      "WARC-Target-URI" -> "my uri",
      "WARC-Date" -> "2016-12-13T03:22:59Z",
      "WARC-Record-ID" -> "<urn:uuid:519aac89-8012-4390-8be6-2d81979f88cb>",
      "WARC-Refers-To" -> "my refers to",
      "WARC-Block-Digest" -> "my block digest",
      "Content-Type" -> "my content type",
      "Content-Length" -> "my content length")

    w.addFields(requiredfields)
    w.addContent("This is my content") 
    w.addWARCInfo(winfo)
    
    assert(w.isComplete() === true)
  }

  "WARCConversion" should "headersComplete returns true when all headers are populated but no content" in {
    // First create the WARCInfo we need
    val winfo = new WARCInfo()
    winfo.addFields(warcinforequired)
    winfo.addContent("This is my content") 
 
    val w: WARCConversion = new WARCConversion()

    val requiredfields: Map[String,String] = Map[String,String](
      "WARC-Type" -> "conversion",
      "WARC-Target-URI" -> "my uri",
      "WARC-Date" -> "2016-12-13T03:22:59Z",
      "WARC-Record-ID" -> "<urn:uuid:519aac89-8012-4390-8be6-2d81979f88cb>",
      "WARC-Refers-To" -> "my refers to",
      "WARC-Block-Digest" -> "my block digest",
      "Content-Type" -> "my content type",
      "Content-Length" -> "my content length")

    assert(w.headersComplete() === false)

    w.addFields(requiredfields)
    
    assert(w.headersComplete() === true)
  }

  "WARCConversion" should "should know content length once headers are complete" in {
    // First create the WARCInfo we need
    val winfo = new WARCInfo()
    winfo.addFields(warcinforequired)
    winfo.addContent("This is my content") 
 
    val w: WARCConversion = new WARCConversion()

    val requiredfields: Map[String,String] = Map[String,String](
      "WARC-Type" -> "conversion",
      "WARC-Target-URI" -> "my uri",
      "WARC-Date" -> "2016-12-13T03:22:59Z",
      "WARC-Record-ID" -> "<urn:uuid:519aac89-8012-4390-8be6-2d81979f88cb>",
      "WARC-Refers-To" -> "my refers to",
      "WARC-Block-Digest" -> "my block digest",
      "Content-Type" -> "my content type",
      "Content-Length" -> "35")

    w.addFields(requiredfields)
    
    assert(w.headersComplete() === true)
    assert(w.getContentSizeInBytes() == 35)
  }

  "WARCConversion" should "should categorize this content as asthma" in {
    // First create the WARCInfo we need
    val winfo = new WARCInfo()
    winfo.addFields(warcinforequired)
    winfo.addContent("This is my content") 
 
    val w: WARCConversion = new WARCConversion()

    val content = "Asthma asthma asthma this should match the asthma category."
    val length = content.getBytes("UTF-8").size.toString()

    val requiredfields: Map[String,String] = Map[String,String](
      "WARC-Type" -> "conversion",
      "WARC-Target-URI" -> "my uri",
      "WARC-Date" -> "2016-12-13T03:22:59Z",
      "WARC-Record-ID" -> "<urn:uuid:519aac89-8012-4390-8be6-2d81979f88cb>",
      "WARC-Refers-To" -> "my refers to",
      "WARC-Block-Digest" -> "my block digest",
      "Content-Type" -> "my content type",
      "Content-Length" -> length)

    w.setMinMentions(4)
    w.addFields(requiredfields)
    w.addContent(content)

    assert(w.hascategories)
    assert(w.getCategories == List[String]("asthma"))
  }

  "WARCConversion" should "should categorize this content as asthma and politics" in {
    // First create the WARCInfo we need
    val winfo = new WARCInfo()
    winfo.addFields(warcinforequired)
    winfo.addContent("This is my content") 
 
    val w: WARCConversion = new WARCConversion()

    val content = "Asthma trump asthma trump asthma trump this should match the asthma and trump politics category."
    val length = content.getBytes("UTF-8").size.toString()

    val requiredfields: Map[String,String] = Map[String,String](
      "WARC-Type" -> "conversion",
      "WARC-Target-URI" -> "my uri",
      "WARC-Date" -> "2016-12-13T03:22:59Z",
      "WARC-Record-ID" -> "<urn:uuid:519aac89-8012-4390-8be6-2d81979f88cb>",
      "WARC-Refers-To" -> "my refers to",
      "WARC-Block-Digest" -> "my block digest",
      "Content-Type" -> "my content type",
      "Content-Length" -> length)

    w.setMinMentions(4)
    w.addFields(requiredfields)
    w.addContent(content)

    assert(w.hascategories)
    assert(w.getCategories.contains("asthma") && w.getCategories.contains("politics") && w.getCategories.size == 2)
  }

  "WARCConversion" should "should not be able to categorize this content" in {
    // First create the WARCInfo we need
    val winfo = new WARCInfo()
    winfo.addFields(warcinforequired)
    winfo.addContent("This is my content") 
 
    val w: WARCConversion = new WARCConversion()

    val content = "This sentence should not match any content categories."
    val length = content.getBytes("UTF-8").size.toString()

    val requiredfields: Map[String,String] = Map[String,String](
      "WARC-Type" -> "conversion",
      "WARC-Target-URI" -> "my uri",
      "WARC-Date" -> "2016-12-13T03:22:59Z",
      "WARC-Record-ID" -> "<urn:uuid:519aac89-8012-4390-8be6-2d81979f88cb>",
      "WARC-Refers-To" -> "my refers to",
      "WARC-Block-Digest" -> "my block digest",
      "Content-Type" -> "my content type",
      "Content-Length" -> length)

    w.setMinMentions(4)
    w.addFields(requiredfields)
    w.addContent(content)

    assert(!w.hascategories)
    assert(w.getCategories.size == 0)
  }


  "WARCConversion" should "should throw an exception if content size is requested before headers are complete" in {
    val w: WARCConversion = new WARCConversion()

    assertThrows[RuntimeException] { w.getContentSizeInBytes() }
  }


  "WARCConversion" should "warcInfoComplete returns true after a completed WARCInfo object is added, false otherwise" in {
    // First create the WARCInfo we need
    val winfo = new WARCInfo()
    winfo.addFields(warcinforequired)
    winfo.addContent("This is my content") 
 
    val w: WARCConversion = new WARCConversion()

    assert(w.warcInfoComplete() === false)

    w.addWARCInfo(winfo)
    
    assert(w.warcInfoComplete() === true)
  }

}
