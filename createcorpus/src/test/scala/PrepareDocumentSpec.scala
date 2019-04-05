import com.jeffharwell.commoncrawl.createcorpus.PrepareDocument
import org.scalatest._

class PrepareDocumentSpec extends FlatSpec {

  /*
   * Cleaning Text Blocks
   */

  val fullquote = """"On the eve of the election last month my wife Judith and I were driving home late in the afternoon and turned on the radio for the traffic and weather. What we instantly got was a freak show of political pornography: lies, distortions, and half-truths -- half-truths being perhaps the blackest of all lies. """"

  /*
   * Pulling junk off the front of a text block
   */
  "preparedocument" should "remove non sentence material from the start of a text block" in {
    val document = """ for the traffic and weather. What we instantly got was a freak show of political pornography: lies, distortions, and half-truths -- half-truths being perhaps the blackest of all lies."""
    val correct = "What we instantly got was a freak show of political pornography: lies, distortions, and half-truths -- half-truths being perhaps the blackest of all lies."

    var prep = new PrepareDocument(document)
    var result = prep.prepare()
    assert(result == correct)
  }

  "preparedocument" should "not throw a java.util.NoSuchElementException when processing this text" in {
    val document = """Leftists
SHARE: Email ThisBlogThis!Share to TwitterShare to FacebookShare to Pinterest
No comments:
"""
    val correct = "SHARE: Email ThisBlogThis!"
    var prep = new PrepareDocument(document)
    //prep.setDebug()
    var result = prep.prepare()
    //println(result)
    assert(result == correct)
  }

  "preparedocument" should "include short lines at the end of a text block" in {
    val document = """Charles Barron didn't lose the race. PUBLIC Education lost today. Remember to say a BIG THANK YOU to the Working Families Party,
  community based organizations, education advocacy groups and all the
  unions that endorsed Jeffries for their great assistance in bringing
  vouchers to NYS!"""
    val correct = """Charles Barron didn't lose the race. PUBLIC Education lost today. Remember to say a BIG THANK YOU to the Working Families Party,
  community based organizations, education advocacy groups and all the
  unions that endorsed Jeffries for their great assistance in bringing
  vouchers to NYS!"""
    var prep = new PrepareDocument(document)
    //prep.setDebug()
    var result = prep.prepare()
    //println(result)
    //println(correct)
    assert(result == correct)
  }  

  "preparedocument" should "include only one short lines at the end of a text block" in {
    val document = """Charles Barron didn't lose the race. PUBLIC Education lost today. Remember to say a BIG THANK YOU to the Working Families Party,
  community based organizations, education advocacy groups and all the
  unions that endorsed Jeffries for their great assistance in bringing
  vouchers to NYS!
  Yes, I'm disgusted."""
    val correct = """Charles Barron didn't lose the race. PUBLIC Education lost today. Remember to say a BIG THANK YOU to the Working Families Party,
  community based organizations, education advocacy groups and all the
  unions that endorsed Jeffries for their great assistance in bringing
  vouchers to NYS!"""
    var prep = new PrepareDocument(document)
    //prep.setDebug()
    var result = prep.prepare()
    //println(result)
    assert(result == correct)
  }  

  "preparedocument" should "not delete this text block" in {
    val document = """“Hakeem Jeffries just seems to be the best prepared,” said voter
  Carla Denalli, 46, of Fort Greene. “He would work much better with                                                   
  the other members of Congress than Charles Barron, who is always
  combative [and] very reminiscent of the boy who cried wolf.”"""
    val correct = """“Hakeem Jeffries just seems to be the best prepared,” said voter
  Carla Denalli, 46, of Fort Greene. “He would work much better with                                                   
  the other members of Congress than Charles Barron, who is always
  combative [and] very reminiscent of the boy who cried wolf.”"""
    var prep = new PrepareDocument(document)
    //prep.setDebug()
    var result = prep.prepare()
    //println("-----")
    //println(result)
    //println("-----")
    assert(result == correct)
  }

  "preparedocument" should "evaluate the last dropped line not delete this text block - test 1" in {
    val document = """In case the point was lost in the moment of triumph, Mr. Jeffries
later told reporters that “the contrast in the race was clear. I
have a record of success and forming coalitions. These are serious
times and Congress is a serious job.”
Mr. Jeffries’s supporters echoed
that sense throughout the day, amid light turnout in the odd June
primary to anoint a Democrat to succeed Rep. Ed Towns, who is
retiring after 30 years in Congress."""
    val correct = """In case the point was lost in the moment of triumph, Mr. Jeffries
later told reporters that “the contrast in the race was clear. I
have a record of success and forming coalitions. These are serious
times and Congress is a serious job.”
Mr. Jeffries’s supporters echoed
that sense throughout the day, amid light turnout in the odd June
primary to anoint a Democrat to succeed Rep. Ed Towns, who is
retiring after 30 years in Congress."""
    var prep = new PrepareDocument(document)
    //prep.setDebug()
    var result = prep.prepare()
    //println(result)
    assert(result == correct)
  }

  "preparedocument" should "evaluate the last dropped line and not delete this text block - test 2" in {
    val document = """Mr. Barron pronounced himself “the best candidate,” adding, “At
  the risk of sounding like a sore loser, there will be no
  congratulatory statement made because the other candidate ran a                                                      
  smear campaign [and] showed a lack of character.”"""
    val correct = """Mr. Barron pronounced himself “the best candidate,” adding, “At
  the risk of sounding like a sore loser, there will be no
  congratulatory statement made because the other candidate ran a                                                      
  smear campaign [and] showed a lack of character.”"""
    var prep = new PrepareDocument(document)
    //prep.setDebug()
    var result = prep.prepare()
    //println("-----")
    //println(result)
    //println(correct)
    //println("-----")
    assert(result == correct)
  }

  "preparedocument" should "not include a dropped starting line if the non-dropped line is the beginning of a sentence / starts with a capital letter" in {
    val document = """EDUCATE! ORGANIZE!! MOBILIZE!!!
These are the three pillars on which Ed Notes is founded – providing information on current ed issues, organizing activities around fighting for public education in NYC and beyond and exposing the motives behind the education deformers. We are part of a tiny band of resisters. Nothing will change unless YOU GET INVOLVED IN THE STRUGGLE!"""
    val correct = """These are the three pillars on which Ed Notes is founded – providing information on current ed issues, organizing activities around fighting for public education in NYC and beyond and exposing the motives behind the education deformers. We are part of a tiny band of resisters. Nothing will change unless YOU GET INVOLVED IN THE STRUGGLE!"""
    var prep = new PrepareDocument(document)
    //prep.setDebug()
    var result = prep.prepare()
    //println("-----")
    //println(result)
    //println(correct)
    //println("-----")
    assert(result == correct)
  }

  "preparedocument" should "not accept text that ends in two or more periods as a complete sentence" in {
    var document = """MORE Endorses National Resolution on Super High Stakes Issue T.."""
    var prep = new PrepareDocument(document)
    //prep.setDebug()
    var result = prep.prepare()
    assert(result == "")
  }

  "preparedocument" should "not keep a fragment that ends in two or more periods on the end of a text block" in {
    var document = """Be Warned, Avoid This School At All Costs.
One of the worst schools for teachers to find themselves in is William Cullen Bryant High School in Queens. This school has one of the l..."""
    var correct = """Be Warned, Avoid This School At All Costs.
One of the worst schools for teachers to find themselves in is William Cullen Bryant High School in Queens."""
    var prep = new PrepareDocument(document)
    //prep.setDebug()
    var result = prep.prepare()
    //println("-----")
    //println(result)
    //println(correct)
    //println("-----")
    assert(result == correct)
  }



  "preparedocument" should "not keep the beginning and ending of this document" in {
    val document = """ more...
Comments are welcome. Irrelevant and abusive comments will be deleted, as will all commercial links. Comment moderation is on, so if your comment does not appear it is because I have not been at my computer (I do not do cell phone moderating).
Newer Post"""
    var correct ="""Comments are welcome. Irrelevant and abusive comments will be deleted, as will all commercial links. Comment moderation is on, so if your comment does not appear it is because I have not been at my computer (I do not do cell phone moderating)."""
    var prep = new PrepareDocument(document)
    var result = prep.prepare()
    assert(result == correct)
  }

  /*
   * Pulling junk off the end of a text block
   */
  /*
  "preparedocument" should "remove non sentence material from the end of a text block" in {
    val document = """On the eve of the election last month my wife Judith and I were driving home late in the afternoon and turned on the radio for the traffic and weather. What we instantly got was a freak sh"""
    var correct = "On the eve of the election last month my wife Judith and I were driving home late in the afternoon and turned on the radio for the traffic and weather."

    var prep = new PrepareDocument(document)
    //prep.setDebug()
    var result = prep.prepare()
    //println("-------")
    //println(result)
    //println("-------")

    assert(result == correct)
  }
  */

  /*
   * Dealing with Strange Ending Quotes
   */
  "preparedocument" should "handle a space between the quote and sentence terminator" in {
    val document = """nutrition and hydration will be taken away."
--MICHAEL SCHIAVO
"On the eve of the election last month my wife Judith and I were driving home late in the afternoon and turned on the radio for the traffic and weather. What we instantly got was a freak show of political pornography: lies, distortions, and half-truths -- half-truths being perhaps the blackest of all lies. "
--BILL MOYERS
"I hate the Republicans an"""
    val correct = """"On the eve of the election last month my wife Judith and I were driving home late in the afternoon and turned on the radio for the traffic and weather. What we instantly got was a freak show of political pornography: lies, distortions, and half-truths -- half-truths being perhaps the blackest of all lies. """"
    var prep = new PrepareDocument(document)
    //prep.setDebug()
    var result = prep.prepare()
    //println("-------")
    //println(result)
    //println("-------")
    assert(result == correct)
  }

  "preparedocument" should "handle return Some(63) as the start of the following complex block" in {
    val document = """nutrition and hydration will be taken away."
--MICHAEL SCHIAVO
"On the eve of the election last month my wife Judith and I were driving home late in the afternoon and turned on the radio for the traffic and weather. What we instantly got was a freak show of political pornography: lies, distortions, and half-truths -- half-truths being perhaps the blackest of all lies. "
--BILL MOYERS
"I hate the Republicans an"""
    val correct = """"On the eve of the election last month my wife Judith and I were driving home late in the afternoon and turned on the radio for the traffic and weather. What we instantly got was a freak show of political pornography: lies, distortions, and half-truths -- half-truths being perhaps the blackest of all lies. """"
    var prep = new PrepareDocument("Dummy Document")
    //prep.setDebug()
    var start_index: Option[Int] = prep.getStartIndexWithAdditionalStartCharacters(document, "\"", "On")
    assert(start_index.isDefined && start_index.get == 63)
  }

  "preparedocument" should "erase a line that does not qualify as a sentence because of lack of capitalization" in {
    val document = """nutrition and hydration will be taken away."
--MICHAEL SCHIAVO"""
    val correct = ""
    var prep = new PrepareDocument(document)
    var result = prep.prepare()
    assert(result == correct)
  }
  "preparedocument" should "properly handle sentences that use straight double quotes" in {
    val document = """start blah blah \u0022Nutrition and hydration will be taken away.\u0022 blah blah"""
    val correct = """\u0022Nutrition and hydration will be taken away.\u0022"""
    var prep = new PrepareDocument(document)
    var result = prep.prepare()
    assert(result == correct)
  }
  "preparedocument" should "properly handle sentences that use unicode curled double quotes" in {
    val document = "start blah blah \u201cNutrition and hydration will be taken away.\u201d blah blah"
    val correct = "\u201cNutrition and hydration will be taken away.\u201d"
    var prep = new PrepareDocument(document)
    var result = prep.prepare()
    assert(result == correct)
  }
  "preparedocument" should "properly handle sentences that use unicode left and right single quotes" in {
    val document = "start blah blah \u2018Nutrition and hydration will be taken away.\u2019 blah blah"
    val correct = "\u2018Nutrition and hydration will be taken away.\u2019"
    var prep = new PrepareDocument(document)
    //prep.setDebug()
    var result = prep.prepare()
    //println("-----")
    //println(result)
    //println("-----")
    assert(result == correct)
  }



  /*
   * Dealing with Strang Starting Quotes
   */
  "preparedocument" should "handle special characters between the start of the sentence and a quotation" in {
    val document = """"[Tea Party Republicans] have acted like terrorists." --JOE BIDEN"""
    val correct = """"[Tea Party Republicans] have acted like terrorists.""""
    var prep = new PrepareDocument(document)
    //prep.setDebug()
    var result = prep.prepare()
    //println("-------")
    //println(result)
    //println("-------")
    assert(result == correct)
  }

  /*
   * Testing Other Components
   */

  /*
   * Testing the Treebank Token Translator
   */

  "translate_treebank_token" should "translate '' to \"" in {
    var prep = new PrepareDocument("this is a dummy document")
    assert(prep.translateTreebankToken("''") == "\"")
  }
  "translate_treebank_token" should "translate ' to '" in {
    var prep = new PrepareDocument("this is a dummy document")
    assert(prep.translateTreebankToken("'") == "'")
  }
  "translate_treebank_token" should "translate -LRB- to (" in {
    var prep = new PrepareDocument("this is a dummy document")
    assert(prep.translateTreebankToken("-LRB-") == "(")
  }
  "translate_treebank_token" should "translate -LSB- to [" in {
    var prep = new PrepareDocument("this is a dummy document")
    assert(prep.translateTreebankToken("-LSB-") == "[")
  }
  "translate_treebank_token" should "translate -LCB- to {" in {
    var prep = new PrepareDocument("this is a dummy document")
    assert(prep.translateTreebankToken("-LCB-") == "{")
  }

  /*
   * Testing getSentenceEndTokens
   */
  "getSentencEndTokenIndex" should "handle a period and a double quote" in {
    var prep = new PrepareDocument("This is a dummy document")
    var sentence = """"This is my test quote.""""
    assert(prep.findSentenceEndIndex(sentence) == Some(sentence.length - 1))
  }
  "getSentencEndTokenIndex" should "handle a period and a unicode closing quote" in {
    var prep = new PrepareDocument("This is a dummy document")
    var sentence = """have a record of success and forming coalitions. These are serious
times and Congress is a serious job.”
Mr Jeffries’s supporters echoed
"""
    assert(prep.findSentenceEndIndex(sentence) == Some(103))
  }


  /*
   * Testing getSentenceStarterTokens
   */
  /*
   *     // previous_tokens = ["''", "-LRB-"] would return the string "\""
    // previous_tokens = ["-LRB-", "''"] would return the string "\"["
    // previous_tokens = ["-LRB-", "-LCB-", "'"] would return the string "("
   */

  "getSentenceStarterTokens" should "accept [\"''\", \"-LRB-\"] and return \"" in {
    var prep = new PrepareDocument("This is a dummy document")
    var previous = List("''","-LRB-")
    assert(prep.getSentenceAdditionalStartCharacters(previous) == Some("\""))
  }
  "getSentenceStarterTokens" should "accept [\"-LRB-\", \"''\"] and return \"(" in {
    var prep = new PrepareDocument("This is a dummy document")
    var previous = List("-LRB-","''")
    assert(prep.getSentenceAdditionalStartCharacters(previous) == Some("\"("))
  }
  "getSentenceStarterTokens" should "accept [\"-LRB-\", \"-LCB-\", \"'\"] and return (" in {
    var prep = new PrepareDocument("This is a dummy document")
    var previous = List("-LRB-","-LCB-","''")
    assert(prep.getSentenceAdditionalStartCharacters(previous) == Some("("))
  }
  "getSentenceAdditionalStartCharacters" should "accept [`, blah, blah, start] and return '\u2018'" in {
    var prep = new PrepareDocument("dummy")
    var previous = List("`", "blah", "blah", "start")
    assert(prep.getSentenceAdditionalStartCharacters(previous) == Some("\u2018"))
  }

  /*
   * Testing getSentenceStarterPattern
   */
  "getStartIndexWithAdditionalStartCharacters" should "return Some(16) for a string that starts with a left single quote" in {
    val textblock = "start blah blah \u2018Nutrition and hydration will be taken away.\u2019 blah blah"
    var prep = new PrepareDocument("This is a dummy document")
    //prep.setDebug()
    var start_token = "Nutrition"
    var previous_chars = "\u2018"
    var start_index: Option[Int] = prep.getStartIndexWithAdditionalStartCharacters(textblock, previous_chars, start_token)
    assert(start_index.isDefined && start_index.get == 16)
  }
  "getStartIndexWithAdditionalStartCharacters" should "return None if the text block is shorter that the string you are searching for" in {
    var prep = new PrepareDocument("This is a dummy document")
    var start_token = "Somethinglong"
    var textblock = "short"
    var previous_chars = ""
    assert(prep.getStartIndexWithAdditionalStartCharacters(textblock, previous_chars, start_token) == None)
  }
  "getStartIndexWithAdditionalStartCharacters" should "return Some(0) if the text block begins with the string you are searching for and there are no previous characters" in {
    var prep = new PrepareDocument("This is a dummy document")
    var start_token = "Hello"
    var textblock = "Hello World"
    var previous_chars = ""
    var start_index: Option[Int] = prep.getStartIndexWithAdditionalStartCharacters(textblock, previous_chars, start_token)
    assert(start_index.isDefined && start_index.get == 0)
  }
  "getStartIndexWithAdditionalStartCharacters" should "return Some(0) if the text block contains with the starting string and there are no previous characters" in {
    var prep = new PrepareDocument("This is a dummy document")
    var start_token = "Hello"
    var textblock = "Hello World"
    var previous_chars = ""
    var start_index: Option[Int] = prep.getStartIndexWithAdditionalStartCharacters(textblock, previous_chars, start_token)
    assert(start_index.isDefined && start_index.get == 0)
  }
  "getStartIndexWithAdditionalStartCharacters" should "return Some(0) if the text block contains with the starting string and a standard double quote as the start character" in {
    var prep = new PrepareDocument("This is a dummy document")
    var start_token = "Hello"
    var textblock = """"Hello World"""
    var previous_chars = "\""
    var start_index: Option[Int] = prep.getStartIndexWithAdditionalStartCharacters(textblock, previous_chars, start_token)
    assert(start_index.isDefined && start_index.get == 0)
  }
  "getStartIndexWithAdditionalStartCharacters" should "return Some(0) if the text block contains with the starting string and a curved double quote as the start character" in {
    var prep = new PrepareDocument("This is a dummy document")
    //prep.setDebug()
    var start_token = "Hello"
    var textblock = """\u201cHello World"""
    var previous_chars = "\u201c"
    var start_index: Option[Int] = prep.getStartIndexWithAdditionalStartCharacters(textblock, previous_chars, start_token)
    assert(start_index.isDefined && start_index.get == 0)
  }
  "getStartIndexWithAdditionalStartCharacters" should "return Some(0) if the text block contains with the starting string and a left single quotation mark as the start character" in {
    var prep = new PrepareDocument("This is a dummy document")
    //prep.setDebug()
    var start_token = "Hello"
    var textblock = """\u2018Hello World"""
    var previous_chars = "\u2018"
    var start_index: Option[Int] = prep.getStartIndexWithAdditionalStartCharacters(textblock, previous_chars, start_token)
    assert(start_index.isDefined && start_index.get == 0)
  }
  "getStartIndexWithAdditionalStartCharacters" should "return Some(0) if the text block starts with the starting string and a single previous characters with no spaces" in {
    var prep = new PrepareDocument("This is a dummy document")
    var start_token = "Hello"
    var previous_chars = "["
    var textblock = "[Hello] World"
    var start_index: Option[Int] = prep.getStartIndexWithAdditionalStartCharacters(textblock, previous_chars, start_token)
    assert(start_index.isDefined && start_index.get == 0)
  }
  "getStartIndexWithAdditionalStartCharacters" should "return Some(0) if the text block starts with the starting string and a single previous characters with a space between them" in {
    var prep = new PrepareDocument("This is a dummy document")
    //prep.setDebug()
    var start_token = "Hello"
    var previous_chars = "["
    var textblock = "[ Hello] World"
    var start_index: Option[Int] = prep.getStartIndexWithAdditionalStartCharacters(textblock, previous_chars, start_token)
    assert(start_index.isDefined && start_index.get == 0)
  }
  "getStartIndexWithAdditionalStartCharacters" should "return Some(0) if the text block starts with the starting string and multiple previous characters with no spaces" in {
    var prep = new PrepareDocument("This is a dummy document")
    var start_token = "Hello"
    var previous_chars = "\"["
    var textblock = "\"[Hello] World"
    var start_index: Option[Int] = prep.getStartIndexWithAdditionalStartCharacters(textblock, previous_chars, start_token)
    assert(start_index.isDefined && start_index.get == 0)
  }
  "getStartIndexWithAdditionalStartCharacters" should "return Some(0) if the text block starts with the starting string and multiple previous characters with spaces between them" in {
    var prep = new PrepareDocument("This is a dummy document")
    var start_token = "Hello"
    var previous_chars = "\"["
    var textblock = "\" [ Hello] World"
    var start_index: Option[Int] = prep.getStartIndexWithAdditionalStartCharacters(textblock, previous_chars, start_token)
    assert(start_index.isDefined && start_index.get == 0)
  }
  "getStartIndexWithAdditionalStartCharacters" should "return Some(5) if the sentence in the text block starts at index 5 with no previous characters" in {
    var prep = new PrepareDocument("This is a dummy document")
    var start_token = "Hello"
    var previous_chars = ""
    var textblock = "junk Hello World"
    var start_index: Option[Int] = prep.getStartIndexWithAdditionalStartCharacters(textblock, previous_chars, start_token)
    assert(start_index.isDefined && start_index.get == 5)
  }
  "getStartIndexWithAdditionalStartCharacters" should "return Some(5) if the sentence in the text block starts at index 5 with one previous characters" in {
    var prep = new PrepareDocument("This is a dummy document")
    var start_token = "Hello"
    var previous_chars = "["
    var textblock = "junk [Hello World"
    var start_index: Option[Int] = prep.getStartIndexWithAdditionalStartCharacters(textblock, previous_chars, start_token)
    assert(start_index.isDefined && start_index.get == 5)
  }
  "getStartIndexWithAdditionalStartCharacters" should "return Some(5) if the sentence in the text block starts at index 5 with multiple previous characters" in {
    var prep = new PrepareDocument("This is a dummy document")
    var start_token = "Hello"
    var previous_chars = "{{"
    var textblock = "junk {{Hello World"
    var start_index: Option[Int] = prep.getStartIndexWithAdditionalStartCharacters(textblock, previous_chars, start_token)
    assert(start_index.isDefined && start_index.get == 5)
  }
  "getStartIndexWithAdditionalStartCharacters" should "return Some(5) if the sentence in the text block starts at index 5 with multiple previous characters and spaces" in {
    var prep = new PrepareDocument("This is a dummy document")
    var start_token = "Hello"
    var previous_chars = "{{"
    var textblock = "junk { { Hello World"
    var start_index: Option[Int] = prep.getStartIndexWithAdditionalStartCharacters(textblock, previous_chars, start_token)
    assert(start_index.isDefined && start_index.get == 5)
  }
  "getStartIndexWithAdditionalStartCharacters" should "return Some(5) if the sentence in the text block starts at index 5 with multiple previous characters and multiple spaces" in {
    var prep = new PrepareDocument("This is a dummy document")
    var start_token = "Hello"
    var previous_chars = "{{"
    var textblock = "junk { {   Hello} World}"
    var start_index: Option[Int] = prep.getStartIndexWithAdditionalStartCharacters(textblock, previous_chars, start_token)
    assert(start_index.isDefined && start_index.get == 5)
  }
  "getStartIndexWithAdditionalStartCharacters" should "return Some(5) if the sentence in the text block starts at index 5 with a double quote" in {
    var prep = new PrepareDocument("This is a dummy document")
    var start_token = "Hello"
    var previous_chars = "\""
    var textblock = "junk \"Hello World}"
    var start_index: Option[Int] = prep.getStartIndexWithAdditionalStartCharacters(textblock, previous_chars, start_token)
    assert(start_index.isDefined && start_index.get == 5)
  }
  "getStartIndexWithAdditionalStartCharacters" should "return Some(5) if the sentence in the text block starts at index 5 with a double quote and a space" in {
    var prep = new PrepareDocument("This is a dummy document")
    var start_token = "Hello"
    var previous_chars = "\""
    var textblock = "junk \" Hello World}"
    var start_index: Option[Int] = prep.getStartIndexWithAdditionalStartCharacters(textblock, previous_chars, start_token)
    assert(start_index.isDefined && start_index.get == 5)
  }

  /*
   * Testing the matchTwoCharacters function which deal with unicode double quotes
   */
  "matchTwoCharacters" should "return true if two non-double quote characters are the same" in {
    var prep = new PrepareDocument("This is a dummy document")
    assert(prep.matchTwoCharacters('a','a'))
  }
  "matchTwoCharacters" should "return false if two non-double quote characters are not same" in {
    var prep = new PrepareDocument("This is a dummy document")
    assert(!prep.matchTwoCharacters('a','b'))
  }
  "matchTwoCharacters" should "return true if two standard double quote characters are passed" in {
    var prep = new PrepareDocument("This is a dummy document")
    assert(prep.matchTwoCharacters('\u0022','\u0022'))
  }
  "matchTwoCharacters" should "return true if a standard double quote and a open double quote are passed" in {
    var prep = new PrepareDocument("This is a dummy document")
    assert(prep.matchTwoCharacters('\u0022','\u201c'))
  }
  "matchTwoCharacters" should "return true if a open double quote and a standard double quote are passed" in {
    var prep = new PrepareDocument("This is a dummy document")
    assert(prep.matchTwoCharacters('\u201c','\u0022'))
  }
  "matchTwoCharacters" should "return true if a standard double quote and a closing double quote are passed" in {
    var prep = new PrepareDocument("This is a dummy document")
    assert(prep.matchTwoCharacters('\u0022','\u201d'))
  }
  "matchTwoCharacters" should "return true if a closing double quote and a standard double quote are passed" in {
    var prep = new PrepareDocument("This is a dummy document")
    assert(prep.matchTwoCharacters('\u201d','\u0022'))
  }
  "matchTwoCharacters" should "return true if a closing double quote and an open double quote are passed" in {
    var prep = new PrepareDocument("This is a dummy document")
    assert(prep.matchTwoCharacters('\u201d','\u201c'))
  }
  "matchTwoCharacters" should "return true if an open double quote and a closing double quote are passed" in {
    var prep = new PrepareDocument("This is a dummy document")
    assert(prep.matchTwoCharacters('\u201c','\u201d'))
  }
  "matchTwoCharacters" should "return true if a grave ` and a left single quote ‘ are passed" in {
    var prep = new PrepareDocument("This is a dummy document")
    assert(prep.matchTwoCharacters('\u0060','\u2018'))
  }
  "matchTwoCharacters" should "return true if a left single quote ‘ and a grave ` are passed" in {
    var prep = new PrepareDocument("This is a dummy document")
    assert(prep.matchTwoCharacters('\u0060','\u2018'))
  }
  "matchTwoCharacters" should "return true if an apostrophe ' and a right single quote ’ are passed" in {
    var prep = new PrepareDocument("This is a dummy document")
    assert(prep.matchTwoCharacters('\'','\u2019'))
  }
  "matchTwoCharacters" should "return true if a right single quote ’ and an apostrophe ' are passed" in {
    var prep = new PrepareDocument("This is a dummy document")
    assert(prep.matchTwoCharacters('\u2019','\''))
  }

  //
  // Testing the adjacent period detection functions
  //
  "hasPeriodPrevious" should "return true if the string has a period previous to the index passed" in {
    var prep = new PrepareDocument("This is a dummy document")
    var a = "hello.."
    assert(prep.hasPeriodPrevious(a, 6))
  }
  "hasPeriodPrevious" should "return false if the string does not have a period previous to the index passed" in {
    var prep = new PrepareDocument("This is a dummy document")
    var a = "hello-."
    assert(!prep.hasPeriodPrevious(a, 6))
  }
  "hasPeriodPrevious" should "return false if the index passed is at the beginning of the string" in {
    var prep = new PrepareDocument("This is a dummy document")
    var a = "hello-."
    assert(!prep.hasPeriodPrevious(a, 0))
  }
  "hasPeriodNext" should "return true if the string has a period after index passed" in {
    var prep = new PrepareDocument("This is a dummy document")
    var a = "hello.."
    assert(prep.hasPeriodNext(a, 5))
  }
  "hasPeriodNext" should "return false if the string does not have a period after the index passed" in {
    var prep = new PrepareDocument("This is a dummy document")
    var a = "hello.-"
    assert(!prep.hasPeriodNext(a, 5))
  }
  "hasPeriodNext" should "return false if the index passed is at the end of the string" in {
    var prep = new PrepareDocument("This is a dummy document")
    var a = "hello-."
    assert(!prep.hasPeriodNext(a, a.length-1))
  }

  // Testing adjustIndexForAdditionalCharacters
  "adjustIndexForAdditionalCharacters" should "pick up an additional valid ending character after the previously identified ending" in {
    var prep = new PrepareDocument("This is a dummy document")
    var a = "\u201cThis is my sentence.\u201d"
    assert(prep.adjustIndexForAdditionalCharacters(a, 20) == 21)
  }
  "adjustIndexForAdditionalCharacters" should "pick up two additional valid ending character after the previously identified ending" in {
    var prep = new PrepareDocument("This is a dummy document")
    var a = "\u201cThis is my (sentence.)\u201d"
    assert(prep.adjustIndexForAdditionalCharacters(a, 21) == 23)
  }
  "adjustIndexForAdditionalCharacters" should "ignore a space after the previously identified ending" in {
    var prep = new PrepareDocument("This is a dummy document")
    var a = "\u201cThis is my (sentence. "
    assert(prep.adjustIndexForAdditionalCharacters(a, 21) == 21)
  }
  "adjustIndexForAdditionalCharacters" should "pick up an additional valid ending characters after a space" in {
    var prep = new PrepareDocument("This is a dummy document")
    var a = "\u201cThis is my (sentence. \u201d"
    assert(prep.adjustIndexForAdditionalCharacters(a, 21) == 23)
  }
  "adjustIndexForAdditionalCharacters" should "ignore spaces when adjusting the index to include additional valid ending charcaters" in {
    var prep = new PrepareDocument("This is a dummy document")
    var a = "\u201cThis is my (sentence. \u201d )"
    assert(prep.adjustIndexForAdditionalCharacters(a, 21) == 25)
  }






}
