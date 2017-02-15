import org.scalatest._
import Matchers._
import com.jeffharwell.commoncrawl.createcorpus.WARCRecord
import com.jeffharwell.commoncrawl.createcorpus.WARCInfo
import com.jeffharwell.commoncrawl.createcorpus.WARCConversion
//import scala.collection.mutable.Map

class WARCRecordSpec extends FlatSpec {

  // Default warcinfo type required fields and values
  val warcinforequired = Map[String,String](
     "WARC-Date" -> "2016-12-13T03:22:59Z"
    ,"WARC-Filename" -> "CC-MAIN-20161202170900-00009-ip-10-31-129-80.ec2.internal.warc.wet.gz"
    ,"WARC-Record-ID" -> "<urn:uuid:519aac89-8012-4390-8be6-2d81979f88cb>"
    ,"Content-Type" -> "application/warc-fields"
    ,"Content-Length" -> "259")

  "WARCInfo Object" should "have these 6 specific required fields" in {
    val requiredfields: List[String] = List[String](
						  "WARC-Date",
						  "WARC-Filename",
						  "WARC-Record-ID",
						  "Content-Type",
						  "Content-Length",
						  "Content")
    
    val warcinfo = new WARCInfo()
    assert(requiredfields.toSet == warcinfo.requiredfields.toSet)
  }

  "WARCConversion Object" should "have these 8 specific required fields" in {
    val requiredfields: List[String] = List[String](
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

  "WARCInfo Object" should "report that it has 6 required fields at initialization" in {
    val warc = new WARCInfo()
    assert(warc.numberRequiredFields == 6)
  }
  "WARCConversion Object" should "report that it has 8 required fields at initialization" in {
    val warc = new WARCConversion()
    assert(warc.numberRequiredFields == 8)
  }


  "Instance of WARCRecord" should "allow optional fields" in {
    // If the WETRecord is passed a field it doesn't know about it should ignore
    // it.
    val w = new WARCInfo()

    w.addFields(Map("NOT-A-WARC-FIELD" -> "Not a WARC field value either"))
    
    assert(w.containsField("NOT-A-WARC-FIELD") === true)
    assert(w.isComplete() === false)
  }

  "WARCInfo" should "be complete with these fields and content" in {
    val w = new WARCInfo()

    val requiredfields: Map[String,String] = Map[String,String](
      "WARC-Date" -> "2016-12-13T03:22:59Z",
      "WARC-Filename" -> "CC-MAIN-20161202170900-00009-ip-10-31-129-80.ec2.internal.warc.wet.gz",
      "WARC-Record-ID" -> "<urn:uuid:519aac89-8012-4390-8be6-2d81979f88cb>",
      "Content-Type" -> "my content type",
      "Content-Length" -> "15")

    w.addFields(requiredfields)
    w.addContent("This is my content") 
    
    assert(w.isComplete() === true)
  }


  "WARCInfo" should "headersComplete returns true when all headers are populated but no content" in {
    val w = new WARCInfo()

    val requiredfields: Map[String,String] = Map[String,String](
      "WARC-Date" -> "2016-12-13T03:22:59Z",
      "WARC-Filename" -> "CC-MAIN-20161202170900-00009-ip-10-31-129-80.ec2.internal.warc.wet.gz",
      "WARC-Record-ID" -> "<urn:uuid:519aac89-8012-4390-8be6-2d81979f88cb>",
      "Content-Type" -> "my content type",
      "Content-Length" -> "15")

    w.addFields(requiredfields)
    //w.addContent("This is my content") 
    
    assert(w.headersComplete() === true)
  }

  "WARCInfo" should "should know content length once headers are complete" in {
    val w = new WARCInfo()

    val requiredfields: Map[String,String] = Map[String,String](
      "WARC-Date" -> "2016-12-13T03:22:59Z",
      "WARC-Filename" -> "CC-MAIN-20161202170900-00009-ip-10-31-129-80.ec2.internal.warc.wet.gz",
      "WARC-Record-ID" -> "<urn:uuid:519aac89-8012-4390-8be6-2d81979f88cb>",
      "Content-Type" -> "my content type",
      "Content-Length" -> "15")

    w.addFields(requiredfields)
    assert(w.headersComplete() === true)
    assert(w.getContentSizeInBytes() == 15)
  }

  "WARCInfo" should "should throw an exception if content size is requested before headers are complete" in {
    val w = new WARCInfo()

    assertThrows[RuntimeException] { w.getContentSizeInBytes() }
  }



  "WARCConversion" should "be complete with these fields and content" in {
    // First create the WARCInfo we need
    val winfo = new WARCInfo()
    winfo.addFields(warcinforequired)
    winfo.addContent("This is my content") 
 
    val w: WARCConversion = new WARCConversion()

    val requiredfields: Map[String,String] = Map[String,String](
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

  "WARCRecord" should "allow you to add a required field" in {
    val w = new WARCInfo()

    w.addFields(warcinforequired)
    w.addContent("This is my content")

    // make sure it is true before adding the new requirement
    assert(w.isComplete() === true)
    w.addRequirement("NEW-Required-Field")
   
    // now that we have added the new requirement it should be false
    assert(w.isComplete() === false)
  }

  "WARCecord" should "be complete when custom required field is added" in {
    val w = new WARCInfo()

    w.addFields(warcinforequired)
    w.addContent("This is my content")

    // make sure it is true before adding the new requirement
    assert(w.isComplete() === true)
    w.addRequirement("NEW-Required-Field")
    w.addFields(Map("NEW-Required-Field" -> "test"))
    
    assert(w.isComplete() === true)
  }

}
