import org.scalatest._
import Matchers._
import com.jeffharwell.commoncrawl.warcparser.WARCRecord
import com.jeffharwell.commoncrawl.warcparser.WARCInfo
import com.jeffharwell.commoncrawl.warcparser.WARCRecordTypeException
import com.jeffharwell.commoncrawl.warcparser.WARCConversion
import com.jeffharwell.commoncrawl.warcparser.MyWARCTopicFilter
//import scala.collection.mutable.Map

class WARCConversionSpec extends FlatSpec {

  // Default warcinfo type required fields and values
  val warcinforequired = Map[String,String](
     "WARC-Type" -> "warcinfo"
    ,"WARC-Date" -> "2016-12-13T03:22:59Z"
    ,"WARC-Filename" -> "CC-MAIN-20161202170900-00009-ip-10-31-129-80.ec2.internal.warc.wet.gz"
    ,"WARC-Record-ID" -> "<urn:uuid:600aac89-8012-4390-8be6-2d81979f88cc>"
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
    
    val warc = WARCConversion()
    assert(requiredfields.toSet == warc.requiredfields.toSet)
  }

  "WARCConversion Object" should "report that it has 9 required fields at initialization" in {
    val warc = WARCConversion()
    assert(warc.numberRequiredFields == 9)
  }

  "WARCConversion" should "throw WARCRecordTypeException if WARC-Type is not 'conversion'" in {
    val w = WARCConversion()

    assertThrows[WARCRecordTypeException] { w.addFields(Map("WARC-Type" -> "warcinfo")) }
  }
  
  "WARCConversion" should "be incomplete without a WARCInfo object" in {
    val w: WARCConversion = WARCConversion()

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
    
    assert(w.isComplete() === false)
  }

  "WARCConversion" should "be complete with these fields and content" in {
    // First create the WARCInfo we need
    val winfo = new WARCInfo()
    winfo.addFields(warcinforequired)
    winfo.addContent("This is my content") 
 
    val w: WARCConversion = WARCConversion()

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


  "WARCConversion" should "return the top level domain of the record if the WARC-URI field is populated" in {
    // First create the WARCInfo we need
    val winfo = new WARCInfo()
    winfo.addFields(warcinforequired)
    winfo.addContent("This is my content") 
 
    val w: WARCConversion = WARCConversion()

    val requiredfields: Map[String,String] = Map[String,String](
      "WARC-Type" -> "conversion",
      "WARC-Target-URI" -> "http://003.su/search_results.php?letter=%D0%9C&offset=580",
      "WARC-Date" -> "2016-12-13T03:22:59Z",
      "WARC-Record-ID" -> "<urn:uuid:519aac89-8012-4390-8be6-2d81979f88cb>",
      "WARC-Refers-To" -> "my refers to",
      "WARC-Block-Digest" -> "my block digest",
      "Content-Type" -> "my content type",
      "Content-Length" -> "my content length")

    w.addFields(requiredfields)
    w.addContent("This is my content") 
    w.addWARCInfo(winfo)

    assert(w.get("Top-Level-Domain") == Some("su"))
    assert(w.isComplete() === true)
  }

  "WARCConversion" should "return None for the top level domain of the record if the WARC-URI field is garbage" in {
    // First create the WARCInfo we need
    val winfo = new WARCInfo()
    winfo.addFields(warcinforequired)
    winfo.addContent("This is my content") 
 
    val w: WARCConversion = WARCConversion()

    val requiredfields: Map[String,String] = Map[String,String](
      "WARC-Type" -> "conversion",
      "WARC-Target-URI" -> "blahblahblah",
      "WARC-Date" -> "2016-12-13T03:22:59Z",
      "WARC-Record-ID" -> "<urn:uuid:519aac89-8012-4390-8be6-2d81979f88cb>",
      "WARC-Refers-To" -> "my refers to",
      "WARC-Block-Digest" -> "my block digest",
      "Content-Type" -> "my content type",
      "Content-Length" -> "my content length")

    w.addFields(requiredfields)
    w.addContent("This is my content") 
    w.addWARCInfo(winfo)

    assert(w.get("Top-Level-Domain") == None)
    assert(w.isComplete() === true)
  }


  "WARCConversion" should "headersComplete returns true when all headers are populated but no content" in {
    // First create the WARCInfo we need
    val winfo = new WARCInfo()
    winfo.addFields(warcinforequired)
    winfo.addContent("This is my content") 
 
    val w: WARCConversion = WARCConversion()

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
 
    val w: WARCConversion = WARCConversion()

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

  "WARCConversion" should "should return the fields it has been populated with" in {
    // First create the WARCInfo we need
    val winfo = new WARCInfo()
    winfo.addFields(warcinforequired)
    winfo.addContent("This is my content") 
 
    val w: WARCConversion = WARCConversion()

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
    w.addContent("This is my WARCConversion content")
    
    assert(w.headersComplete() === true)
    assert(w.getContentSizeInBytes() == 35)
    assert(w.get("Content-Type") == Some("my content type"))
    assert(w.get("WARC-Date") == Some("2016-12-13T03:22:59Z"))
    assert(w.get("WARC-Type") == Some("conversion"))
    assert(w.getContent() == Some("This is my WARCConversion content"))
  }

  "WARCConversion" should "should return the fields from WARCInfo using the get method" in {
    // First create the WARCInfo we need
    val winfo = new WARCInfo()
    winfo.addFields(warcinforequired)
    winfo.addContent("This is my content") 
 
    val w: WARCConversion = WARCConversion()

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
    w.addContent("This is my WARCConversion content")
    w.addWARCInfo(winfo)
 
    // The WARC-Filename field is in the WARCInfo object, not directly in the wARCConversion object's fields.
    assert(w.get("WARC-Filename") == Some("CC-MAIN-20161202170900-00009-ip-10-31-129-80.ec2.internal.warc.wet.gz"))
  }

  "WARCConversion" should "should return None for non-existent field even in the absence of a WARCInfo object" in {
    val w: WARCConversion = WARCConversion()

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
    w.addContent("This is my WARCConversion content")
 
    // The WARC-Filename field is in the WARCInfo object, not directly in the wARCConversion object's fields.
    w.get("WARC-Filename") should be (None)
  }


  "WARCConversion" should "Fields from WARCConversion should mask fields from WARCInfo if they have the same name" in {
    // First create the WARCInfo we need
    val winfo = new WARCInfo()
    winfo.addFields(warcinforequired)
    winfo.addContent("This is my content") 
 
    val w: WARCConversion = WARCConversion()

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
    w.addContent("This is my WARCConversion content")
    w.addWARCInfo(winfo)
   
    // The WARCConversion record has a WARC-Record-ID and so does the WARCInfo object that
    // we passed. <urn:uuid:519aac89-8012-4390-8be6-2d81979f88cb> is the WARC conversion ID
    // while <urn:uuid:600aac89-8012-4390-8be6-2d81979f88cc> is the WARC info ID
    assert(w.get("WARC-Record-ID") == Some("<urn:uuid:519aac89-8012-4390-8be6-2d81979f88cb>"))
  }


  "WARCConversion" should "should throw an exception if content size is requested before headers are complete" in {
    val w: WARCConversion = WARCConversion()

    assertThrows[RuntimeException] { w.getContentSizeInBytes() }
  }


  "WARCConversion" should "warcInfoComplete returns true after a completed WARCInfo object is added, false otherwise" in {
    // First create the WARCInfo we need
    val winfo = new WARCInfo()
    winfo.addFields(warcinforequired)
    winfo.addContent("This is my content") 
 
    val w: WARCConversion = WARCConversion()

    assert(w.warcInfoComplete() === false)

    w.addWARCInfo(winfo)
    
    assert(w.warcInfoComplete() === true)
  }

  /*
   * Testing Categorizers
   */

  "WARCConversion" should "should categorize this content as asthma with MyWARCCategorizer" in {
    // First create the WARCInfo we need
    val winfo = new WARCInfo()
    winfo.addFields(warcinforequired)
    winfo.addContent("This is my content") 
 
    val c: MyWARCTopicFilter = new MyWARCTopicFilter(4)
    val w: WARCConversion = WARCConversion(c)

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

    w.addFields(requiredfields)
    w.addContent(content)

    assert(w.hasCategories)
    assert(w.getCategories == Some(Set[String]("asthma")))
  }

  "WARCConversion" should "should categorize this content as asthma and politics with MyWARCCategorizer" in {
    // First create the WARCInfo we need
    val winfo = new WARCInfo()
    winfo.addFields(warcinforequired)
    winfo.addContent("This is my content") 
 
    val c: MyWARCTopicFilter = new MyWARCTopicFilter(4)
    val w: WARCConversion = WARCConversion(c)

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

    w.addFields(requiredfields)
    w.addContent(content)

    assert(w.hasCategories)
    w.getCategories() shouldBe defined
    //w.getCategories() should contain "asthma"
    //w.getCategories() should contain "politics"
    assert(w.getCategories().get.size == 2)

    //assert(w.getCategories.contains("asthma") && w.getCategories.contains("politics") && w.getCategories.size == 2)
  }

  "WARCConversion" should "should not be able to categorize this content with MyWARCCategorizer" in {
    // First create the WARCInfo we need
    val winfo = new WARCInfo()
    winfo.addFields(warcinforequired)
    winfo.addContent("This is my content") 
 
    val c: MyWARCTopicFilter = new MyWARCTopicFilter(4)
    val w: WARCConversion = WARCConversion(c)

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

    w.addFields(requiredfields)
    w.addContent(content)

    assert(!w.hasCategories)
    w.getCategories() should be (None)
  }

  "WARCConversion" should "should not categorize this content with default categorizer" in {
    // First create the WARCInfo we need
    val winfo = new WARCInfo()
    winfo.addFields(warcinforequired)
    winfo.addContent("This is my content") 
 
    val w: WARCConversion = WARCConversion()

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

    w.addFields(requiredfields)
    w.addContent(content)

    assert(!w.hasCategories)
    w.getCategories() should be (None)
  }

}
