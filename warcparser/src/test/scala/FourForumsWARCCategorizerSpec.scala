import org.scalatest._
import com.jeffharwell.commoncrawl.warcparser.FourForumsWARCCategorizer

class FourForumsWARCCategorizerSpec extends FlatSpec {

  /*
 * Unit Tests
 */

  "FourForumsWARCCategorizer" should "not categorize a paragraph that only contains core keywords" in {
    val c = new FourForumsWARCCategorizer(1, 1)
    assert(!c.hasCategories(testcontext1_1))
  }

  "FourForumsWARCCategorizer" should "categorize testcontext1 with two core keywords and one secondary words" in {
    val c = new FourForumsWARCCategorizer(2, 1)
    assert(c.hasCategories(testcontext1))
  }

  "FourForumsWARCCategorizer" should "categorize testcontext2 with two core keywords and three secondary keywords" in {
    val c = new FourForumsWARCCategorizer(1, 1)
    assert(c.getCategories(testcontext2).size > 0)
  }

  "FourForumsWARCCategorizer" should "not categorize a paragraph that only contains two core keyword with minmentions = 3" in {
    val c = new FourForumsWARCCategorizer(3, 1)
    assert(!c.hasCategories(testcontext2))
  }

  "FourForumsWARCCategorizer" should "categorize this paragraph as guncontrol" in {
    val c = new FourForumsWARCCategorizer(1, 1)
    assert(c.getCategories(testcontext2) == Set("guncontrol"))
  }

  "FourForumsWARCCategorizer" should "categorize this paragraph as existenceofgod with one core keyword and one secondary keyword" in {
    val c = new FourForumsWARCCategorizer(1, 1)
    assert(c.getCategories(testcontext6) == Set("existenceofgod"))
  }

  "FourForumsWARCCategorizer" should "not categorize this paragraph as existenceofgod with two core keyword and one secondary keyword" in {
    val c = new FourForumsWARCCategorizer(2, 1)
    assert(c.getCategories(testcontext6).isEmpty)
  }

  "FourForumsWARCCategorizer" should "not categorize this paragraph as existenceofgod with one core keyword and four secondary keyword" in {
    val c = new FourForumsWARCCategorizer(1, 4)
    assert(c.getCategories(testcontext6).isEmpty)
  }

  "FourForumsWARCCategorizer" should "count one core keyword and three secondary keyword in test string 6 for the existenceofgod category" in {
    val c = new FourForumsWARCCategorizer(1, 4)
    val result_tuple = c.categorizeAndCountString(testcontext6)
    assert(result_tuple._1.isEmpty) // no category
    assert(result_tuple._2("existenceofgod")._1 == 1)
    assert(result_tuple._2("existenceofgod")._2 == 3)
    assert(c.getCategories(testcontext6) != Set("existenceofgod"))
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
}