import org.scalatest._
import com.jeffharwell.commoncrawl.warcparser.{FourForumsWARCTopicFilter, WARCConversion, WARCInfo}

class FourForumsWARCTopicFilterSpec extends FlatSpec {

  // Default warcinfo type required fields and values
  val warcinforequired: Map[String, String] = Map[String,String](
    "WARC-Type" -> "warcinfo"
    ,"WARC-Date" -> "2016-12-13T03:22:59Z"
    ,"WARC-Filename" -> "CC-MAIN-20161202170900-00009-ip-10-31-129-80.ec2.internal.warc.wet.gz"
    ,"WARC-Record-ID" -> "<urn:uuid:600aac89-8012-4390-8be6-2d81979f88cc>"
    ,"Content-Type" -> "application/warc-fields"
    ,"Content-Length" -> "259")

  def create_warc_record(content: String): WARCConversion = {
    // First create the WARCInfo we need
    val winfo = new WARCInfo()
    winfo.addFields(warcinforequired)
    winfo.addContent("This is my content")

    val w: WARCConversion = WARCConversion()

    val requiredfields: Map[String, String] = Map[String, String](
      "WARC-Type" -> "conversion",
      "WARC-Target-URI" -> "my uri",
      "WARC-Date" -> "2016-12-13T03:22:59Z",
      "WARC-Record-ID" -> "<urn:uuid:519aac89-8012-4390-8be6-2d81979f88cb>",
      "WARC-Refers-To" -> "my refers to",
      "WARC-Block-Digest" -> "my block digest",
      "Content-Type" -> "my content type",
      "Content-Length" -> s"${content.length}")

    w.addFields(requiredfields)
    w.addContent(content)
    w.addWARCInfo(winfo)
    w
  }

  /*
 * Unit Tests
 */

  "FourForumsWARCTopicFilter" should "throw an IllegalArgumentException if asked to set a mentions threshhold for a topic that does not exist" in {
    val c = new FourForumsWARCTopicFilter()
    assertThrows[IllegalArgumentException] {
      c.setMentions("bogustopic", Map("core" -> 1, "secondary" -> 1))
    }
  }

  "FourForumsWARCTopicFilter" should "throw an IllegalArgumentException if the 'core' key is not included in the mentions map" in {
    val c = new FourForumsWARCTopicFilter()
    assertThrows[IllegalArgumentException] {
      c.setMentions("bogustopic", Map("secondary" -> 1))
    }
  }

  "FourForumsWARCTopicFilter" should "throw an IllegalArgumentException if the 'secondary' key is not included in the mentions map" in {
    val c = new FourForumsWARCTopicFilter()
    assertThrows[IllegalArgumentException] {
      c.setMentions("bogustopic", Map("core" -> 1))
    }
  }

  "FourForumsWARCTopicFilter" should "not categorize a paragraph that only contains core keywords" in {
    val c = new FourForumsWARCTopicFilter()
    val w = create_warc_record(testcontext1_1)
    //assert(!c.hasCategories(testcontext1_1))
    assert(!c.hasCategories(w))
  }

  "FourForumsWARCTopicFilter" should "categorize testcontext1 with two core keywords and one secondary words" in {
    val c = new FourForumsWARCTopicFilter()
    c.setMentions("guncontrol", Map("core" -> 2, "secondary" -> 1))
    val w = create_warc_record(testcontext1)
    //assert(c.hasCategories(testcontext1))
    assert(c.hasCategories(w))
  }

  "FourForumsWARCTopicFilter" should "categorize testcontext2 with two core keywords and three secondary keywords" in {
    val c = new FourForumsWARCTopicFilter()
    c.setMentions("guncontrol", Map("core" -> 2, "secondary" -> 3))
    //assert(c.getCategories(testcontext2).nonEmpty)
    assert(c.getCategories(create_warc_record(testcontext2)).nonEmpty)
  }

  "FourForumsWARCTopicFilter" should "not categorize a paragraph that only contains two core keyword with minmentions = 3" in {
    val c = new FourForumsWARCTopicFilter()
    c.setMentions("guncontrol", Map("core" -> 3, "secondary" -> 2))
    val w = create_warc_record(testcontext2)
    //assert(!c.hasCategories(testcontext2))
    assert(!c.hasCategories(w))
  }

  "FourForumsWARCTopicFilter" should "categorize this paragraph as guncontrol" in {
    val c = new FourForumsWARCTopicFilter()
    c.setMentions("guncontrol", Map("core" -> 1, "secondary" -> 1))
    val w = create_warc_record(testcontext2)
    //assert(c.getCategories(testcontext2) == Set("guncontrol"))
    assert(c.getCategories(w) == Set("guncontrol"))
  }

  "FourForumsWARCTopicFilter" should "categorize this paragraph as existenceofgod with one core keyword and one secondary keyword" in {
    val c = new FourForumsWARCTopicFilter()
    c.setMentions("existenceofgod", Map("core" -> 1, "secondary" -> 1))
    val w = create_warc_record(testcontext6)
    //assert(c.getCategories(testcontext6) == Set("existenceofgod"))
    assert(c.getCategories(w) == Set("existenceofgod"))
  }

  "FourForumsWARCTopicFilter" should "not categorize this paragraph as existenceofgod with three core keyword and one secondary keyword" in {
    val c = new FourForumsWARCTopicFilter()
    c.setMentions("existenceofgod", Map("core" -> 3, "secondary" -> 1))
    val w = create_warc_record(testcontext6)
    //assert(c.getCategories(testcontext6).isEmpty)
    assert(c.getCategories(w).isEmpty)
  }

  "FourForumsWARCTopicFilter" should "not categorize this paragraph as existenceofgod with one core keyword and five secondary keyword" in {
    val c = new FourForumsWARCTopicFilter()
    c.setMentions("existenceofgod", Map("core" -> 1, "secondary" -> 5))
    val w = create_warc_record(testcontext6)
    assert(c.getCategories(w).isEmpty)
  }

  "FourForumsWARCTopicFilter" should "count two core keyword and four secondary keyword in test string 6 for the existenceofgod category" in {
    val c = new FourForumsWARCTopicFilter()
    c.setMentions("existenceofgod", Map("core" -> 10, "secondary" -> 10))
    val result_tuple = c.categorizeAndCountString(testcontext6)
    assert(result_tuple._1.isEmpty) // no category
    assert(result_tuple._2("existenceofgod")._1 == 2)
    assert(result_tuple._2("existenceofgod")._2 == 4)
    val w = create_warc_record(testcontext6)
    assert(c.getCategories(w) != Set("existenceofgod"))
  }

  "FourForumsWARCTopicFilter" should "match segundo to gun by default" in {
    val c = new FourForumsWARCTopicFilter()
    c.setMentions("guncontrol", Map("core" -> 1, "secondary" -> 0))
    val w = create_warc_record(testcontext7)
    assert(c.hasCategories(w))
  }

  "FourForumsWARCTopicFilter" should "match a short sentence with the word gun with core -> 1" in {
    val c = new FourForumsWARCTopicFilter()
    c.setMentions("guncontrol", Map("core" -> 1, "secondary" -> 0))
    val w = create_warc_record(testcontext8)
    assert(c.hasCategories(w))
  }

  "FourForumsWARCTopicFilter" should "count one core keyword in short sentence ending with the keyword" in {
    val c = new FourForumsWARCTopicFilter()
    c.setMentions("guncontrol", Map("core" -> 10, "secondary" -> 10))
    val result_tuple = c.categorizeAndCountString(testcontext9)
    assert(result_tuple._1.isEmpty) // no category
    assert(result_tuple._2("guncontrol")._1 == 1)
    assert(result_tuple._2("guncontrol")._2 == 0)
  }

  "FourForumsWARCTopicFilter" should "count one core keyword in short sentence beginning with the keyword" in {
    val c = new FourForumsWARCTopicFilter()
    c.setMentions("guncontrol", Map("core" -> 10, "secondary" -> 10))
    val result_tuple = c.categorizeAndCountString(testcontext10)
    assert(result_tuple._1.isEmpty) // no category
    assert(result_tuple._2("guncontrol")._1 == 1)
    assert(result_tuple._2("guncontrol")._2 == 0)
  }

  "FourForumsWARCTopicFilter" should "count one secondary keyword in short sentence ending with the keyword" in {
    val c = new FourForumsWARCTopicFilter()
    c.setMentions("guncontrol", Map("core" -> 10, "secondary" -> 10))
    val result_tuple = c.categorizeAndCountString(testcontext11)
    assert(result_tuple._1.isEmpty) // no category
    assert(result_tuple._2("guncontrol")._1 == 1)
    assert(result_tuple._2("guncontrol")._2 == 1)
  }

  "FourForumsWARCTopicFilter" should "count one secondary keyword in short sentence beginning with the keyword" in {
    val c = new FourForumsWARCTopicFilter()
    c.setMentions("guncontrol", Map("core" -> 10, "secondary" -> 10))
    val result_tuple = c.categorizeAndCountString(testcontext12)
    assert(result_tuple._1.isEmpty) // no category
    assert(result_tuple._2("guncontrol")._1 == 1)
    assert(result_tuple._2("guncontrol")._2 == 1)
  }



  /*
  Testing Require Token Separator function
   */

  "FourForumsWARCTopicFilter" should "not match segundo to gun if RequireTokenSeparator is set" in {
    val c = new FourForumsWARCTopicFilter()
    c.setRequireTokenSeparator(true)
    c.setMentions("guncontrol", Map("core" -> 1, "secondary" -> 0))
    val w = create_warc_record(testcontext7)
    assert(!c.hasCategories(w))
  }

  "FourForumsWARCTopicFilter" should "count one core keyword in short sentence ending with the keyword as a subword without token separator required" in {
    val c = new FourForumsWARCTopicFilter()
    c.setMentions("guncontrol", Map("core" -> 10, "secondary" -> 10))
    c.setRequireTokenSeparator(false)
    val result_tuple = c.categorizeAndCountString(testcontext13)
    assert(result_tuple._1.isEmpty) // no category
    assert(result_tuple._2("guncontrol")._1 == 1)
    assert(result_tuple._2("guncontrol")._2 == 0)
  }

  "FourForumsWARCTopicFilter" should "count one core keyword in short sentence beginning with the keyword as a subword without token separator required" in {
    val c = new FourForumsWARCTopicFilter()
    c.setMentions("guncontrol", Map("core" -> 10, "secondary" -> 10))
    c.setRequireTokenSeparator(false)
    val result_tuple = c.categorizeAndCountString(testcontext14)
    assert(result_tuple._1.isEmpty) // no category
    assert(result_tuple._2("guncontrol")._1 == 1)
    assert(result_tuple._2("guncontrol")._2 == 0)
  }

  "FourForumsWARCTopicFilter" should "not count one core keyword in short sentence ending with the keyword as a subword with token separator required" in {
    val c = new FourForumsWARCTopicFilter()
    c.setMentions("guncontrol", Map("core" -> 10, "secondary" -> 10))
    c.setRequireTokenSeparator(true)
    val result_tuple = c.categorizeAndCountString(testcontext13)
    assert(result_tuple._1.isEmpty) // no category
    assert(result_tuple._2("guncontrol")._1 == 0)
    assert(result_tuple._2("guncontrol")._2 == 0)
  }

  "FourForumsWARCTopicFilter" should "not count one core keyword in short sentence beginning with the keyword as a subword with token separator required" in {
    val c = new FourForumsWARCTopicFilter()
    c.setMentions("guncontrol", Map("core" -> 10, "secondary" -> 10))
    c.setRequireTokenSeparator(true)
    val result_tuple = c.categorizeAndCountString(testcontext14)
    assert(result_tuple._1.isEmpty) // no category
    assert(result_tuple._2("guncontrol")._1 == 0)
    assert(result_tuple._2("guncontrol")._2 == 0)
  }

  "FourForumnsWARCTopicFilter" should "convert multiple categories into a JSON string" in {
    def get_list_of_categories(s: String): List[String] = {
      // Because JSON is not a first class citizen in Scala, and I don't want to add a dependency
      // on some giant framework like Lift, we will just do the parse by hand. It is a simple
      // pattern
      //
      // The category string looks something like: "{'existenceofgod','guncontrol'}" which needs
      // to be converted to a list List('existenceofgod', 'guncontrol')
      s.split("','").map(x => x.replaceAll("[}{']", "")).toList
    }

    val c = new FourForumsWARCTopicFilter()
    c.setMentions("existenceofgod", Map("core" -> 1, "secondary" -> 0))
    c.setMentions("guncontrol", Map("core" -> 1, "secondary" -> 0))
    val categories_string = c.categorizeAndCountStringReturnString(testcontext15)
    val categories_list = get_list_of_categories(categories_string)
    assert(categories_list.contains("existenceofgod"))
    assert(categories_list.contains("guncontrol"))
    assert(categories_list.size == 2)
  }

  "FourForumnsWARCTopicFilter" should "return the string {} when there are no categories" in {
    val c = new FourForumsWARCTopicFilter()
    c.setMentions("existenceofgod", Map("core" -> 10, "secondary" -> 0))
    val categories_string = c.categorizeAndCountStringReturnString(testcontext14)
    assert(categories_string == "{}")
  }

  /*
 * Here is the sample data used to test filter. It consists of a few samples
 * taken from the web.
 */

  def testcontext1 =
    """
      |The Second Amendment to the U.S. Constitution gives Americans the right to bear arms, and about a third of U.S. adults say they personally own a gun. At the same time, President Joe Biden and other policymakers earlier this year proposed new restrictions on firearm access in an effort to address gun violence ranging from rising murder rates in some major cities to mass shootings.
      |""".stripMargin
  def testcontext1_1 =
    """
      |The Second Amendment to the U.S. Constitution gives Americans the right to bear arms, and about a third of U.S. adults say they personally own a gun. At the same time, President Joe Biden and other policymakers earlier this year proposed new restri--ions on firearm access in an effort to address gun violence ranging from rising murder rates in some major cities to mass shootings.
      |""".stripMargin

  def testcontext2 =
    """
      |Roughly half of Americans (53%) favor stricter gun laws, a decline since 2019, according to the Center’s April 2021 survey. Smaller shares say these laws are about right (32%) or should be less strict (14%). The share of Americans who say gun laws should be stricter has decreased from 60% in September 2019. Current opinions are in line with what they were in March 2017.
      |""".stripMargin

  def testcontext3 =
    """
      |Americans' support for stricter gun control has fallen five percentage points to 52%, the lowest reading since 2014. At the same time, 35% of U.S. adults think laws covering the sale of firearms should be kept as they are now and 11% favor less strict laws.
      |""".stripMargin

  def testcontext4 =
    """
      |Sentence about guns and nothing else.
      |""".stripMargin

  def testcontext5: String =
    """
      |Strict strict strict control control control.
      |""".stripMargin

  def testcontext6: String =
    """
      |The atheistic conclusion is that the arguments and evidence both indicate there is insufficient reason to believe that any gods exist,
      |and that personal subjective religious experiences say something about the human experience rather than the nature of reality itself;
      |therefore, one has no reason to believe that a god exists.""".stripMargin

  def testcontext7: String =
    """
      |primero segundo tercero.
      |""".stripMargin

  def testcontext8: String =
    """
      |short gun sent
      |""".stripMargin

  def testcontext9: String =
    """
      |short sent gun
      |""".stripMargin

  def testcontext10: String =
    """
      |gun short sent
      |""".stripMargin

  def testcontext11: String =
    """
      |short sent gun control
      |""".stripMargin

  def testcontext12: String =
    """
      |control gun short sent
      |""".stripMargin

  def testcontext13: String =
    """
      |short sent segundo
      |""".stripMargin

  def testcontext14: String =
    """
      |segundo short sent
      |""".stripMargin

  def testcontext15 =
    """
      |Sentence about guns and god.
      |""".stripMargin
}
