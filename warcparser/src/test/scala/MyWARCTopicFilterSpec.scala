import org.scalatest._
import com.jeffharwell.commoncrawl.warcparser.Parser
import com.jeffharwell.commoncrawl.warcparser.WARCConversion
import com.jeffharwell.commoncrawl.warcparser.WARCInfo
import com.jeffharwell.commoncrawl.warcparser.MyWARCTopicFilter

class MyWARCTopicFilterSpec extends FlatSpec {

 /*
 * Unit Tests
 */

  "MyWARCTopicFilter" should "assign a topic to a paragraph that only mentions the keyword once with minmentions = 1" in
  {
    val c = new MyWARCTopicFilter(1)
    assert(c.getCategories(testcontent1).size > 0)

  }

  "MyWARCTopicFilter" should "assign the topic asthma to testcontent1 minmentions = 1" in
  {
    val c = new MyWARCTopicFilter(1)
    assert(c.getCategories(testcontent1) == Set("asthma"))
  }


  "MyWARCTopicFilter" should "not assign a topic to a paragraph that only mentions the keyword once with minmentions = 2" in
  {
    val c = new MyWARCTopicFilter(2)
    assert(!c.hasCategories(testcontent1))
  }

/*
 * Here is the sample data used to test filter. It consists of a few samples
 * taken from. 
 * CC-MAIN-20161202170900-00009-ip-10-31-129-80.ec2.internal.warc.wet.gz
 *
 * You can also find the complete WARC records in the resource file filter_test_1.wet.gz
 *
 * One of the nastiest problems with this content is how to divide sentences. I 
 * tried to grab sample content the illustrates this problem
 *
 * Wanting to accept the text from mixperiodandcarriagereturn, but reject the text
 * from asthmaweather while also rejecting the text from keywordstuffing illustrates 
 * the tension well.
 */

  def testcontent1 = """
Theres a CrossFit affiliate gym 4 blocks from my house. Everytime I drive by there and want to stop in, Im intimidated by the men and women beasting out there lol. I soooo want to do this. Im going to start on some of the excercises you can do at home. But does anyone recommend any lifting regiments for a starter? I have never lifted really but have decent upper body strength. Im going to need a scaled suggestion. I know the 1-1-1-1-1-1-1 to start out. But is that 1 rep, 7 sets of lifting excercises like curls. Then repeat the 1-1-1-1-1-1-1 for say triceps? Is there a specific order of lifting excercises/motions I should be doing? Sorry for the super noob questions. Ive never really worked out hardcore or had a set regiment due to hardcore asthma I used to have. Its since gotten way better than when I was a kid, but still there. I still play soccer, go jogging, etc. But I NEED something more. I read through a whole bunch of pages, but wasnt able to get through all 93 lol. HELP! Thanks!
"""

  def taglist = """
TagsAir
Air Pollution
air quality
air research
Around the Water Cooler
asthma
children's health
climate
climate change
Community Voices
drinking water
earth day
Education
energy
energy star
environment
Environmental Justice
epa
Green Infrastructure
Green Solutions
Human Health
In Your Backyard
Land
Local Community
"""

def keywordstuffing = """
TagsAir Air Pollution air quality air research Around the Water Cooler asthma children's health climate climate change Community Voices drinking water earth day Education energy energy star environment Environmental Justice epa Green Infrastructure Green Solutions Human Health In Your Backyard Land Local Community outdoor activities Partnership pesticides recycle recycling science Science Wednesday spanish Stormwater sustainability sustainable communities sustainable living urban escapes waste Wastewater water water quality Water Quality WaterSense Watersheds What you can do"""

def asthmaweather = """
weather for asthma
Hi RealFeel: 71°
UV: 4 - Moderate
Air Qual.: Good
More
Thu
Dec 8
51°
/33°F
Sunny and cooler
Beneficial
weather for asthma
Hi RealFeel: 49°
UV: 4 - Moderate
Air Qual.: Good
Fri
Dec 9
54°
/35°
Plenty of sunshine
Beneficial
"""
def mixperiodandcarriagereturn = """
The
lack of certain enzymes causes very specific problems. A lack of
sugar-digesting enzymes--sucrase, maltase and lactase--can even lead to
severe problems such as diarrhea and asthma. The lack of these
enzymes has also been linked to attention-deficit disorder, mood swings,
violent behavior and other mental and emotional problems.
"""

def mayoclinic = """
Asthma Research - Mayo Clinic
Diseases and ConditionsAsthma
By Mayo Clinic Staff
Asthma is a condition in which your airways narrow and swell and produce extra mucus. This can make breathing difficult and trigger coughing, wheezing and shortness of breath.
For some people, asthma is a minor nuisance. For others, it can be a major problem that interferes with daily activities and may lead to a life-threatening asthma attack.
Asthma can't be cured, but its symptoms can be controlled. Because asthma often changes over time, it's important that you work with your doctor to track your signs and symptoms and adjust treatment as needed.Asthma symptoms vary from person to person. You may have infrequent asthma attacks, have symptoms only at certain times — such as when exercising — or have symptoms all the time.
"""

}

