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

  /*
   * Dealing with Strang Starting Quotes
   */
  "preparedocument" should "handle special characters between the start of the sentence and a quotation" in {
    val document = """"[Tea Party Republicans] have acted like terrorists." --JOE BIDEN"""
    val correct = """"[Tea Party Republicans] have acted like terrorists.""""
    var prep = new PrepareDocument(document)
    prep.setDebug()
    var result = prep.prepare()
    println("-------")
    println(result)
    println("-------")
    assert(result == correct)
  }


}
