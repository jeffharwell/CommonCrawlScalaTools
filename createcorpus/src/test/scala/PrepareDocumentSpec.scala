import com.jeffharwell.commoncrawl.createcorpus.PrepareDocument
import scala.collection.mutable.ListBuffer
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

  "preparedocument" should "not throw a java.util.NoSuchElementException when processing this text, but instead return an empty string" in {
    val document = """Leftists
SHARE: Email ThisBlogThis!Share to TwitterShare to FacebookShare to Pinterest
No comments:
"""
    //val correct = "SHARE: Email ThisBlogThis!"
    // If we are requiring a space or line ending (or a few other valid characters) after a sentence terminator
    // then this block doesn't qualify at all.
    val correct = ""
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
    //println(document)
    //println("--")
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
    //println("--")
    //println(correct)
    //println("-----")
    assert(result == correct)
  }

  "preparedocument" should "keep this as a complete text block even though the middle line does not qualify to be kept" in {
    var document = """“I am committed to the public school system, and we will support                                                     
public schools and parochials schools,” he said. He also said he would work to help homeowners who are at risk of    
foreclosure. East New York, Canarsie and Bedford-Stuyvesant — all
within the Eighth Congressional District — are particularly hard
hit, he said."""
    var correct = """“I am committed to the public school system, and we will support                                                     
public schools and parochials schools,” he said. He also said he would work to help homeowners who are at risk of    
foreclosure. East New York, Canarsie and Bedford-Stuyvesant — all
within the Eighth Congressional District — are particularly hard
hit, he said."""
    var prep = new PrepareDocument(document)
    //prep.setDebug()
    var result = prep.prepare()
    //println("-----")
    //println(result)
    //println(correct)
    //println("-----")
    assert(result == correct)
  }

  "preparedocument" should "keep this as a complete text block even though the middle line does not qualify to be kept - test 2" in {
    var document = """Assemblyman Alec Brook-Krasny, who represents Brighton Beach and
its large Russian population, said Mr. Barron’s rhetoric on Israel
helped earn Mr. Jeffries strong support among Russian Jews.
“I’ve never seen such strong numbers,” said Mr. Brook-Krasny. “We
have some election districts with votes of 44–0, 115–2, 32–2. It’s
because, one, we had a great candidate, but also, two, that
Charles Barron has opinions about Israel that the Russian Jews do
not find suitable for Israel. It’s like he’s a foreign enemy.”"""
    var correct = """Assemblyman Alec Brook-Krasny, who represents Brighton Beach and
its large Russian population, said Mr. Barron’s rhetoric on Israel
helped earn Mr. Jeffries strong support among Russian Jews.
“I’ve never seen such strong numbers,” said Mr. Brook-Krasny. “We
have some election districts with votes of 44–0, 115–2, 32–2. It’s
because, one, we had a great candidate, but also, two, that
Charles Barron has opinions about Israel that the Russian Jews do
not find suitable for Israel. It’s like he’s a foreign enemy.”"""
    var prep = new PrepareDocument(document)
    //prep.setDebug()
    var result = prep.prepare()
    //println("-----")
    //println(result)
    //println("--")
    //println(correct)
    //println("-----")
    assert(result == correct)
  }

  "preparedocument" should "not add a rejected interstitial line or previously dropped line onto the beginning of the next textblock if that textblock starts with a capital letter" in {
    val document = """Yes, I'm disgusted.
Read the article below:
June 27, 2012, 1:03 am Comment
Hakeem Jeffries Defeats Charles Barron in
Bitter Democratic Primary
By GERSH
KUNTZMAN
Gersh Kuntzman (left) and
Matthew J. Perlman Assemblyman
Hakeem Jeffries beat Councilman Charles Barron for the
Democratic nomination for Congress yesterday, tantamount to
election in deeply Democratic Brooklyn."""
    var correct = """Gersh Kuntzman (left) and
Matthew J. Perlman Assemblyman
Hakeem Jeffries beat Councilman Charles Barron for the
Democratic nomination for Congress yesterday, tantamount to
election in deeply Democratic Brooklyn."""
    var prep = new PrepareDocument(document)
    //prep.setDebug()
    var result = prep.prepare()
    //println("-----")
    //println(result)
    //println("--")
    //println(correct)
    //println("-----")
    assert(result == correct)
  }

  "preparedocument" should "not include a dropped initial line if the start of the textblock is already a potential start of a sentence" in {
    val document = """KUNTZMAN
Gersh Kuntzman (left) and
Matthew J. Perlman Assemblyman
Hakeem Jeffries beat Councilman Charles Barron for the
Democratic nomination for Congress yesterday, tantamount to
election in deeply Democratic Brooklyn."""
    var correct = """Gersh Kuntzman (left) and
Matthew J. Perlman Assemblyman
Hakeem Jeffries beat Councilman Charles Barron for the
Democratic nomination for Congress yesterday, tantamount to
election in deeply Democratic Brooklyn."""
    var prep = new PrepareDocument(document)
    //prep.setDebug()
    var result = prep.prepare()
    //println("-----")
    //println(result)
    //println("--")
    //println(correct)
    //println("-----")
    assert(result == correct)
  }

  "preparedocument" should "not consider a single letter and a period as a valid sentence ending" in {
    val document = "Harry Reid: James Comey is the New J. Edgar Hoover"
    val correct = ""
    var prep = new PrepareDocument(document)
    var result = prep.prepare()
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

  "preparedocument" should "not see a period in a URL as a valid sentence ending" in {
    val document = """Great Post on Teacher Quality at the Morton School                                                                   
I'm very tired of the myth that schools are bursting at the seams with apathetic, unskilled, surly, child-hating losers who can't get jobs doing anything else. I recently figured that, counting high school and college where one encounters many teachers in the course of a year, I had well over 100 teachers in my lifetime, and I can only say that one or two truly had no place being in a classroom.More at: http://themortonschool.blogspot.com/2009/07/its-teachers-stupidright.html
The Randi Sellout Tour
"""
    val correct = """Great Post on Teacher Quality at the Morton School                                                                   
I'm very tired of the myth that schools are bursting at the seams with apathetic, unskilled, surly, child-hating losers who can't get jobs doing anything else."""
    var prep = new PrepareDocument(document)
    //prep.setDebug()
    var result = prep.prepare()
    assert(result == correct)
  }

  "preparedocument" should "reject middle lines that are 100% non-alphanumeric" in {
    val document = """Outrage on the Page | notes from a teacher educator                                                                  
The PARCC Test: Exposed [excerpts deleted under legal threat from Parcc]                                             
-                                                                                                                    
The author of this blog posting is a public school teacher who will remain anonymous. I will not reveal my …
"""
    val correct = """The author of this blog posting is a public school teacher who will remain anonymous."""
    var prep = new PrepareDocument(document)
    //prep.setDebug()
    var result = prep.prepare()
    //println(result)
    //println("---")
    //println(correct)
    assert(result == correct)
  }

  "preparedocument" should "reject this text block as being too long to only have one valid sentence ending" in {
    val document = """January 10th, 2014 | Tags: Boehner, death penalty, filibuster, ObamaCare, property rights, quotas, RINOs, Supreme Court | Category: Constitution, Courts, Legal, Crime, Criminal Justice, Elections, Campaigns, Healthcare, public health, Politics, Race, Ethnicity, The Left, The Right | Leave a comment widget @ surfing-waves.com The politically motivated, wrongful prosecution of Rick Renzi
New AG Sessions and Congress Must Investigate DOJ Corruption in the Case of Rep. Rick Renzi
Rick Renzi Puts Together Top Legal Team to Appeal Hidden Evidence of FBI Agent's Corruption
Judge Unbelievably Refuses to Grant a Retrial for Former Rep. Renzi Despite Finding Rampant Prosecutorial Wrongdoing
Bombshell: New Evidence Reveals Prosecutor Corruption in Trial Against Former Congressman Rick Renzi
Time For a Congressional Investigation: Shattering New Developments of Corruption in Rep. Renzi Trial
Judge Unravels Illegal Activity by Prosecution That Ensured a Conviction of Renzi — But Will he do Anything About it?"""
    val correct = ""
    var prep = new PrepareDocument(document)
    //prep.setDebug()
    var result = prep.prepare()
    //println(result)
    //println("---")
    //println(correct)
    assert(result == correct)
  }

  /*
   * Pulling junk off the end of a text block
   */
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

  "preparedocument" should "return an empty string when processing this text snippet" in {
    val document = """Ted Cruz
(54)
Ted Cruz. Ted Cruz Rally North Idaho
(1)
Ted Nugent
(1)"""
    val correct = ""
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
    var tokens = prep.tokenize_line(sentence)
    assert(prep.findSentenceEndIndex(sentence, tokens) == Some(sentence.length - 1))
  }
  "getSentencEndTokenIndex" should "handle a period and a unicode closing quote" in {
    var prep = new PrepareDocument("This is a dummy document")
    var sentence = """have a record of success and forming coalitions. These are serious
times and Congress is a serious job.”
Mr Jeffries’s supporters echoed
"""
    var tokens = prep.tokenize_line(sentence)
    assert(prep.findSentenceEndIndex(sentence, tokens) == Some(103))
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

  //
  // Testing the function hasSpaceOrEndingNext()
  "hasSpaceOrEndingNext" should "return true if the index passed is at the end of the string" in {
    var prep = new PrepareDocument("This is a dummy document")
    var a = "hello-."
    assert(prep.hasSpaceOrEndingNext(a, a.length-1))
  }
  "hasSpaceOrEndingNext" should "return true if the index passed has a space at index + 1" in {
    var prep = new PrepareDocument("This is a dummy document")
    var a = "hello-. World"
    assert(prep.hasSpaceOrEndingNext(a, 6))
  }
  "hasSpaceOrEndingNext" should "return false if the index passed does not have a space at index + 1" in {
    var prep = new PrepareDocument("This is a dummy document")
    var a = "hello-.-World"
    assert(!prep.hasSpaceOrEndingNext(a, 6))
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

  // Testing valid sentence endings
  "isValidSentenceEnding" should "reject Mr. as a valid ending" in {
    var prep = new PrepareDocument("This is a dummy document")
    var a = "Test Mr."
    var tokens = prep.tokenize_line(a)
    assert(!prep.isValidSentenceEnding(a, 7, tokens))
  }
  "isValidSentenceEnding" should "reject Ms. as a valid ending" in {
    var prep = new PrepareDocument("This is a dummy document")
    var a = "Test Ms."
    var tokens = prep.tokenize_line(a)

    assert(!prep.isValidSentenceEnding(a, 7, tokens))
  }
  "isValidSentenceEnding" should "reject Jr. as a valid ending" in {
    var prep = new PrepareDocument("This is a dummy document")
    var a = "Test Jr."
    var tokens = prep.tokenize_line(a)
    assert(!prep.isValidSentenceEnding(a, 7, tokens))
  }
  "isValidSentenceEnding" should "reject Sr. as a valid ending" in {
    var prep = new PrepareDocument("This is a dummy document")
    var a = "Test Sr."
    var tokens = prep.tokenize_line(a)
    assert(!prep.isValidSentenceEnding(a, 7, tokens))
  }
  "isValidSentenceEnding" should "reject a period with a period after as a valid ending" in {
    var prep = new PrepareDocument("This is a dummy document")
    var a = "Test Sr.."
    var tokens = prep.tokenize_line(a)
    assert(!prep.isValidSentenceEnding(a, 7, tokens))
  }
  "isValidSentenceEnding" should "reject a period with a period before as a valid ending" in {
    var prep = new PrepareDocument("This is a dummy document")
    var a = "Test Sr.."
    var tokens = prep.tokenize_line(a)
    assert(!prep.isValidSentenceEnding(a, 7, tokens))
  }
  "isValidSentenceEnding" should "reject Ret. as a valid ending" in {
    var prep = new PrepareDocument("This is a dummy document")
    var a = "Test Rev."
    var tokens = prep.tokenize_line(a)
    //println(tokens)
    assert(!prep.isValidSentenceEnding(a, 7, tokens))
  }
  "isValidSentenceEnding" should "reject first initial as a valid ending" in {
    var prep = new PrepareDocument("This is a dummy document")
    var a = "J. R. Tolkin is a writer."
    var tokens = prep.tokenize_line(a)
    assert(!prep.isValidSentenceEnding(a, 4, tokens))
  }
  "isValidSentenceEnding" should "reject any period and a single letter as valid ending" in {
    var prep = new PrepareDocument("This is a dummy document")
    var a = "Test "
    var tokens = prep.tokenize_line(a)
    var letters: Array[String] = "abcdefghijklmnoprstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890".split("")
    letters.foreach(x => assert(!prep.isValidSentenceEnding(a+x+".", 6, tokens+=x+".")))
  }
  "isValidSentenceEnding" should "reject a non-sentence ending character" in {
    var prep = new PrepareDocument("dummy")
    var a = "test"
    var tokens = prep.tokenize_line(a)
    assert(!prep.isValidSentenceEnding(a, 2, tokens))
  }
  "isValidSentenceEnding" should "accept a question mark as an ending character" in {
    var prep = new PrepareDocument("dummy")
    var a = "test?"
    var tokens = prep.tokenize_line(a)
    assert(prep.isValidSentenceEnding(a, 4, tokens))
  }
  "isValidSentenceEnding" should "accept an exclamation mark as an ending character" in {
    var prep = new PrepareDocument("dummy")
    var a = "test!"
    var tokens = prep.tokenize_line(a)
    assert(prep.isValidSentenceEnding(a, 4, tokens))
  }
  "endsWithInvalid" should "return true if the sentence ends with one of the invalid endings in the list" in {
    var invalid: ListBuffer[String] = new ListBuffer[String]()
    invalid += "eone"
    invalid += "etwo"
    var s = "my sentence etwo"
    var prep = new PrepareDocument("")
    assert(prep.endsWithInvalid(s, invalid))
  }
  "endsWithInvalid" should "return false if the sentence does not ends with one of the invalid endings in the list" in {
    var invalid: ListBuffer[String] = new ListBuffer[String]()
    invalid += "eone"
    invalid += "etwo"
    var s = "my sentence etwo."
    var prep = new PrepareDocument("")
    assert(!prep.endsWithInvalid(s, invalid))
  }
}
