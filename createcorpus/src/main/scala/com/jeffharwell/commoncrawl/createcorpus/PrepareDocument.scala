package com.jeffharwell.commoncrawl.createcorpus

import scala.collection.mutable.ListBuffer
import java.io.StringReader
import java.util.regex.Pattern
import edu.stanford.nlp.process.PTBTokenizer
import edu.stanford.nlp.process.CoreLabelTokenFactory
import java.security.MessageDigest
import java.math.BigInteger


/*
 * This class attempts to implement the Common Crawl document cleanup strategy
 * outline by:
 * Laippala, V., & Ginter, F. (2014). Syntactic n-gram collection from a large-scale corpus of internet finnish. In IOS Press (Vol. 268, p. 184).
 */

class PrepareDocument(document: String) {

  /*
   * Deal with counting and classifying tokens
   */
  var alpha_num_pattern = Pattern.compile("[A-Za-z0-9']+")
  var upper_pattern = Pattern.compile("^[A-Z].*")
  var terminator_pattern = Pattern.compile("[.!?][\"']*$")
  var openingdoublequote_pattern = Pattern.compile("``") // Tokenizer converts opening " to `` 
  var closingdoublequote_pattern = Pattern.compile("''") // Tokenizer converts closing " to ''
  var singlequote_pattern = Pattern.compile("'") // Tokenizer leaves single quotes alone
  var numeric_pattern = Pattern.compile("[0-9]+")
  var debug = false

  def setDebug(): Unit = {
    debug = true
  }

  def count_if_alpha_num(t: String): Int = {
    if (alpha_num_pattern.matcher(t).matches()) {
      1
    } else {
      0
    }
  }
  def count_if_numeric(t: String): Int = {
    if (numeric_pattern.matcher(t).matches()) {
      1
    } else {
      0
    }
  }

  def countAlphaNum(tokens: ListBuffer[String]): Int = {
    tokens.map(count_if_alpha_num).reduce(_ + _)
  }

  def countNumeric(tokens: ListBuffer[String]): Int = {
    tokens.map(count_if_numeric).reduce(_ + _)
  }

  /*
   * md5sum hash of a string
   * https://alvinalexander.com/source-code/scala-method-create-md5-hash-of-string
   */
  def md5HashString(s: String): String = {
	  val md = MessageDigest.getInstance("MD5")
	  val digest = md.digest(s.getBytes)
	  val bigInt = new BigInteger(1,digest)
	  val hashedString = bigInt.toString(16)
	  hashedString
  }

  def tokenize_line(line: String): ListBuffer[String] = {
    var line_as_reader = new StringReader(line)
    var ptbt = new PTBTokenizer(line_as_reader, new CoreLabelTokenFactory(), "")
    var tokens = new ListBuffer[String]()
    while (ptbt.hasNext()) {
      var label = ptbt.next()
      if (debug) println(label)
      tokens += label.originalText() //edu.stanford.nlp.ling.CoreLabel
    }
    tokens
  }

  /*
   * We want allow sentencs to start with something like "( or "[. These are converted
   * into tokens ['"', "-LRB-"] but the PTBTokenizer. Need to handle that in the 
   * code that finds the start of sentences
   *
   * https://www.nltk.org/_modules/nltk/tokenize/treebank.html
   *     CONVERT_PARENTHESES = [
   *     (re.compile(r'\('), '-LRB-'),
   *     (re.compile(r'\)'), '-RRB-'),
   *     (re.compile(r'\['), '-LSB-'),
   *     (re.compile(r'\]'), '-RSB-'),
   *     (re.compile(r'\{'), '-LCB-'),
   *     (re.compile(r'\}'), '-RCB-'),
   * ]
   */
  def translateTreebankToken(token: String): String = {
    token match {
      case "-LRB-" => "("
      case "-LSB-" => "["
      case "-LCB-" => "{"
      case "'" => "'"
      case "''" => "\""
      case "``" => "\""
      case "`" => "\u2018"
    }
  }

  var valid_opening_brackets = List[String]("-LRB-","-LSB-","-LCB-")
  var valid_quotes = List[String]("`","'","''","``")
  def getSentenceAdditionalStartCharacters(previous_tokens: List[String], sentence_start: String = ""): Option[String] = {
    // This is a bit tricky, we want to recurse through the previous tokens list.
    // If the first token is an single or double quote we are done. If it is an opening bracket (-LRB-, -LSB-, -LCB-)
    // then we slap that onto the sentence_start variable and then recurse. If the next token is a single or double quote (' or '')
    // then we put that on the front of the sentence_start string and return it.
    // The only valid characters for the first token we parse are opening brackets or quotes. The only valid characters for the 
    // second token we parse are the quotes. If we hit anything else we return None.
    //
    // Example:
    // previous_tokens = ["''", "-LRB-"] would return the string "\""
    // previous_tokens = ["-LRB-", "''"] would return the string "\"["
    // previous_tokens = ["-LRB-", "-LCB-", "'"] would return the string "("
    if (debug) print("Getting the string representation of relevant additional PTB starting characters, looking at: ")
    if (debug) println(previous_tokens)
    if (previous_tokens.length == 0) {
      // well, that was easy
      None
    } else {
      var h = previous_tokens.head
      if ((valid_opening_brackets contains h) && (sentence_start == "")) { // we only accept an opening bracket as the first character before the capitalized token
        // we have a bracket
        var translated: String = translateTreebankToken(h)
        // and recurse
        getSentenceAdditionalStartCharacters(previous_tokens.tail, translated + sentence_start)
      } else if (valid_quotes contains h) {
        // we have a quote
        var translated: String = translateTreebankToken(h)
        // once we hit a quote we are done, return what we have got
        Some(translated + sentence_start)
      } else {
        if (sentence_start == "") {
          None
        } else {
          Some(sentence_start)
        }
      }
    }
  }

  def findSentenceStartIndex(textblock: String, tokens: ListBuffer[String], previous_tokens: List[String] = List[String]()): Option[Int] = {
    var h = tokens.head
    if (upper_pattern.matcher(h).matches()) {
      // alright, we have the token that starts the sentence, now what else could be in front that could
      // also be part of the sentence?
      var other_characters: Option[String] = getSentenceAdditionalStartCharacters(previous_tokens)
      if (other_characters.isDefined) {
        // Trickier, we have tokens that might be part of the start of the sentence. For example the 
        // string "[Bob] did the thing!" previous = "[ and h = Bob. getStartIndexWithAdditionalCharacters
        // handles the case where the textblock actually has the value " [ Bob] did the thing!" and we need
        // to find the index of the opening double quotes.
        getStartIndexWithAdditionalStartCharacters(textblock, other_characters.get, h)
      } else {
        // Easy, there are no valid tokens before the uppercase token that we just matched
        // which might be part of the sentence. So get the index of the token we matched 
        // and just return it.
        Some(textblock indexOf h)
      }
    } else if (tokens.length > 1) { // we did not find an token that starts with a capital letter, but have some tokens left, keep looking
      var new_previous: List[String] = h :: previous_tokens // adds the current head token to the front of the previous tokens list
      findSentenceStartIndex(textblock, tokens.tail, new_previous)
    } else {
      None
    }
  }

  // We have a token that starts the sentence in the text block, say the token 'Bob'. We also have some characters that go before it but belong
  // to the same sentence, say the string '"['. Now, the trick is that the PTB Tokenizer drops spaces as it tokenizes, so the textblock might
  // actually be: 'end of a fragment. " [ Bob] did the thing!"', and we need to find the index of the opening double quote. I'm going to try to
  // just hack this thing together with recursion rather then do a full FSA which would solve this elegantly ... blargh
  def getStartIndexWithAdditionalStartCharacters(textblock: String, previous_characters: String, sentence_start_token: String, search_index: Int = 0, textblock_index: Int = 0, current_match: Int = -1): Option[Int] = {
    if (debug) println("getStartIndexWithAdditionalStartCharacters: Evaluating "+textblock.head)
    if (textblock.length < sentence_start_token.length) {
      // we are done, there is no way the text block can match
      None
    } else if (textblock.head == ' ') {
      // We just skip it and keep rolling
      if (debug) println("Found a space, just keep rolling")
      getStartIndexWithAdditionalStartCharacters(textblock.tail, previous_characters, sentence_start_token, search_index, textblock_index + 1, current_match)
    } else if (textblock.startsWith(sentence_start_token) && (previous_characters == "" || search_index == previous_characters.length)) {
      // we have a full match, previous charcaters plus the full start token
      if (current_match == -1) {
        Some(textblock_index)
      } else {
        Some(current_match)
      }
    } else if (previous_characters == "") {
      // No previous characters and the start_token did match where we are in the text block, move to the next
      // character and try again
      getStartIndexWithAdditionalStartCharacters(textblock.tail, previous_characters, sentence_start_token, search_index, textblock_index + 1, current_match)
    } else if (search_index == previous_characters.length) {
      // We matched all the previous characters but the start token didn't match, so no dice, start over
      getStartIndexWithAdditionalStartCharacters(textblock.tail, previous_characters, sentence_start_token, 0, textblock_index + 1, -1)
    } else if (matchTwoCharacters(textblock.head, previous_characters(search_index))) {
      if (search_index == 0) {
        // we have now matched the very first of our previous characters, set the current_match to our textblock_index, increment the search_index, and keep going
        getStartIndexWithAdditionalStartCharacters(textblock.tail, previous_characters, sentence_start_token, 1, textblock_index + 1, textblock_index)
      } else {
        // just matched another character of the previous characters, increment the search_index and keep going
        getStartIndexWithAdditionalStartCharacters(textblock.tail, previous_characters, sentence_start_token, search_index + 1, textblock_index + 1, current_match)
      }
    } else {
      // no match, reset everything and keep going
      getStartIndexWithAdditionalStartCharacters(textblock.tail, previous_characters, sentence_start_token, 0, textblock_index + 1, -1)
    }
  }

  def matchTwoCharacters(first: Char, second: Char): Boolean = {
    // You would think this would be simple, but .. the PTB tokenizer converts both
    // curly double quotes \u201c and standard double quotes \u0022 into the token ''
    // so when we are comparing the output of the tokenizer with the original text
    // we need to take this into account

    if (first == second) {
      // that was easy
      true
    } else if ((first == '`' || first == '\u2018') && (second == '`' || second == '\u2018')) {
      // Deal with left single quotes
      true
    } else if ((first == '\'' || first == '\u2019') && (second == '\'' || second == '\u2019')) {
      // Deal with right single quotes
      true
    } else if ((first == '"' || first == '\u201c') && (second == '"' || second == '\u201c')) {
      // Deal with right double quotes
      true
    } else if ((first == '"' || first == '\u201d') && (second == '"' || second == '\u201d')) {
      // Deal with left double quotes
      true
    } else if ((first == '\u201d' || first == '\u201c') && (second == '\u201d' || second == '\u201c')) {
      // Deal with case of left and right double quotes being interchanged
      // I'm not sure this would actually come up
      true
    } else {
      false
    }
  }

  // This function looks for any valid "additional characters" that may occur after the 
  // sentence ending punctuation mark (., ?, or !). These include square brackets, round 
  // brackets, and double quotes (including unicode closing quote) and single quotes (including
  // unicode closing single quote).
  def adjustIndexForAdditionalCharacters(textblock: String, ending_index: Int): Int = {
    var valid_additional_ending_characters = List('\u201d','\u2019','"','\'','\u2019',']',')')
    var search_index = ending_index + 1

    def findNewIndex(ending_index: Int, search_index: Int): Int = {
      if (search_index >= textblock.length) {
        // We just overran the end of the string, return whatever we may have found
        ending_index
      } else if (valid_additional_ending_characters.exists(_ == textblock(search_index))) {
        // this could be a contender, set it as the next ending and keep looking
        findNewIndex(search_index, search_index + 1)
      } else if (' ' == textblock(search_index)) {
        // it is a space, evaluate the next character, but don't treat this as a potential
        // end of the sentence
        findNewIndex(ending_index, search_index + 1)
      } else {
        // end of the road. it is not a space or additional character, the sentence ends here
        ending_index
      }
    }
    findNewIndex(ending_index, search_index)
  }

  // Returns true if the character before the index is a period, false otherwise
  def hasPeriodPrevious(textblock: String, current_index: Int): Boolean = {
    if (current_index == 0) {
      false
    } else if (textblock(current_index - 1) == '.') {
      true
    } else {
      false
    }
  }

  // Returns true if the next charcater after the index is a period, false otherwise
  def hasPeriodNext(textblock: String, current_index: Int): Boolean = {
    if (current_index + 1 >= textblock.length) {
      // we are already at the end of the sentence, so no
      false
    } else if (textblock(current_index + 1) == '.') {
      true
    } else {
      false
    }
  }

  // Look through the textblock backwards, character by character, and return the index that marks the end
  // of the last sentence in the textblock..
  def findSentenceEndIndex(textblock: String): Option[Int] = {
    if (textblock.length == 0) {
      None
    }
    def findIndex(textblock: String, index: Int): Option[Int] = {
      if (index == 0) {
        // we went all the way through the sentence and didn't find anything
        None
      } else if (textblock(index) == '.' || textblock(index) == '?' || textblock(index) == '!') {
        // we might have something here, do some more checks
        if (textblock(index) == '.' && (hasPeriodPrevious(textblock, index) || hasPeriodNext(textblock, index))) {
          // we don't except multiple periods as the end of a sentence, keep looking
          findIndex(textblock, index - 1)
        } else {
          // We have a sentence ending, see if there are any additional characters to tack onto
          // sentence (quotes, etc.)
          // We actually return the index right after the last valid end character,
          // so that later functions can slice with abandon
          Some(adjustIndexForAdditionalCharacters(textblock, index))
        }
      } else {
        // we found nothing, but we are not at the end yet, recurse
        findIndex(textblock, index - 1)
      }
    }

    findIndex(textblock, textblock.length - 1)
  }

  // Clean the text block by removing anything from the beginning that does not belong to a sentence
  // (as best we can tell) and anything from the end that does not belong to a sentence (again, as best
  // we can tell ... a lot of this stuff isn't in complete sentences anyways.)
  def cleanTextblock(textblock: String): Option[String] = {
    var tokens = tokenize_line(textblock)
    var start: Option[Int] = findSentenceStartIndex(textblock, tokens)
    if (start.isDefined) {
      if (debug) println(s"Start index is: ${start}")
      var end: Option[Int] = findSentenceEndIndex(textblock)
      if (end.isDefined && start.get <= end.get) {
        if (debug) println(s"End index is: ${end}")
        Some(textblock.slice(start.get, end.get + 1))
      } else {
        if (debug && end.isDefined) println(s"Found end ${end} but it is less than start ${start}, discarding block.")
        if (debug && !end.isDefined) println("Did not found a valid sentence ending. Discarding block.")
        None
      }
    } else {
      if (debug) println("No valid sentence start was found, discarding block.")
      None
    }
  }

  def keep_line(line: String, in_textblock: Boolean): Boolean = {
    var tokens = tokenize_line(line)
    // hmm, this avoids computation until it is absolutely necessary
    // but it is certainly ugly to code. I guess it is a guard type
    // of pattern
    // This is from Laippala and Ginter 2014
    // They discarded lines that were 5 or less tokens, less than 70% alpha
    // (words, alphabetical is my interpretation of not "special character tokens")
    // or greater than 20% numeric
    if (debug) println("Evaluating Line:")
    if (debug) println(line)
    if (in_textblock && detect_sentence_ending(line)) {
      // we are currently in a text block and this looks like the end of a sentence
      // we don't care how short it is, go ahead and keep it
      // I added this huristic because, at least in the documents I tested, it seems that
      // often a text block's last sentence will end with just a word or two on the last line.
      // Without this rule the cleaner will discard the entire text block. 
      if (debug) println("    Accepting line because it looks like the end of the sentence for the current textblock.")
      true
    } else if (tokens.size > 5) {
      var alpha_percent = BigDecimal(countAlphaNum(tokens))/tokens.size
      if (alpha_percent > .7) {
        var numeric_percent = BigDecimal(countNumeric(tokens))/tokens.size
        if (numeric_percent < .2) {
          if (debug) println("    Accepted line")
          true
        } else {
          if (debug) println("    Rejected line: it did not have less than 20% numeric characters")
          false
        }
      } else {
        if (debug) println("    Rejected line: it did not have more than 70% alpha numeric characters")
        false
      }
    } else {
      if (debug) println("    Rejected line: it did not have > 5 tokens") 
      false
    }
  }

  def detect_sentence_ending(line: String): Boolean = {
    var last = line.takeRight(1)
    var penultimate = line.takeRight(2).take(1)
    var antipenultimate = line.takeRight(3).take(1)
    if (last == "." || last == "?" || last == "!") {
      if (line.takeRight(3) == "..") {
        // We don't accept .. as the valid end of a sentence
        false
      } else {
        true
      }
    } else if (last == "'" || last == "\"" || last == "\u201D") {
      if (debug) println("Found a quote")
      if (penultimate == "." || penultimate == "?" || penultimate == "!") {
        true
      } else {
        false
      }
    } else {
      false
    }
  }

  def prepare(): String = {
    // See the API Usage Section of
    // https://nlp.stanford.edu/software/tokenizer.shtml

	var hashes: ListBuffer[String] = new ListBuffer()
    var builder = StringBuilder.newBuilder 
    var textblock: ListBuffer[String] = ListBuffer()
    var in_textblock: Boolean = false
    var last_dropped_line: Option[String] = None

    // a little helper function.
    // This is a bit subtle, if the last line that we dropped looks like it could be the start
    // of a text block (i.e. it begins with a capital letter and the actual text block does not) 
    // then throw it onto the front of the current textblock we are evaluating. Otherwise just 
    // include all of the non-dropped lines which are in the builder.
    def buildTextBlock(last_dropped_line: Option[String], string_builder: StringBuilder): String = {
      if (last_dropped_line.isDefined && upper_pattern.matcher(last_dropped_line.get).matches() && !upper_pattern.matcher(builder.toString()).matches()) {
        if (debug) println("Including last dropped line")
        if (debug) println("    "+last_dropped_line.get)
        last_dropped_line.get + "\n" + builder.toString()
      } else {
        builder.toString()
      }
    }

    for (line <- document.split("\r?\n")) { // maybe a bit simplistic ... but it matches
                                            // matches the LineIterator class from Source.scala, although
                                            // using the much slower Regexp split.
                                            // https://github.com/scala/scala/blob/2.8.x/src/library/scala/io/Source.scala
      // First see if we are keeping the line
      if (keep_line(line, in_textblock)) {
        // Build the hash for the line, used later to detect highly duplicate document
        hashes += md5HashString(line)
        if (builder.length == 0) {
          builder.append(line)
          in_textblock = true
        } else {
          // Throw in a space if we are appending this to an existing line
          // often it seems like, in the WET archives, a new line is replacing
          // HTML code that probably generated a gap in the content
          builder.append("\n"+line)
        }

        if (detect_sentence_ending(line)) {
          // We found the end of a sentenc, write out the text block
          // and start a new one
          if (debug) println("---Is sentence ending")
          var cleaned: Option[String] = cleanTextblock(buildTextBlock(last_dropped_line, builder))
          // the foreach will only run if there is a value in the option
          // Bascially, the potential text block might not even have enough info
          // in it to keep it, the cleaner might basically delete all the content.
          // https://danielwestheide.com/blog/2012/12/19/the-neophytes-guide-to-scala-part-5-the-option-type.html
          cleaned.foreach(tb => textblock.append(tb))
          builder = StringBuilder.newBuilder
          in_textblock = false
          // Since we just processed a text block without dropping a line go ahead and clear
          // the last_dropped_line since we know it will not apply to the next text block
          // we are starting on.
          last_dropped_line = None
        }
      } else {
        // We are dropping a line, the text block ends here
        if (builder.length > 0) {
          // if there is anything in the buffer
          if (debug) println("We dropped a line, ending the text block, processing remaining text in the string builder")
          var cleaned: Option[String] = cleanTextblock(buildTextBlock(last_dropped_line, builder))
          if (debug) {
            if (cleaned.isDefined) {
              println("Cleaner returned content, whatever remained in the string builder did not contain a complete sentence.")
            } else {
              println("Cleaner did not return content")
            }
          }

          // the foreach will only run if there is a value in the option
          // Bascially, the potential text block might not even have enough info
          // in it to keep it, the cleaner might basically delete all the content.
          // https://danielwestheide.com/blog/2012/12/19/the-neophytes-guide-to-scala-part-5-the-option-type.html
          cleaned.foreach(tb => textblock.append(tb)) // the foreach will only run if there is a value
          builder = StringBuilder.newBuilder
          in_textblock = false
        }
        // We are dropping this line. Let's see if we might want to keep it for the next 
        // text block
        if (upper_pattern.matcher(line).matches()) {
          // Hmm, could be a short or complex sentence that is actually the start
          // of the next text block, keep it just in case.
          if (debug) println("Setting last_dropped_line")
          last_dropped_line = Some(line)
        } else {
          // Probably not useful, don't keep it around
          last_dropped_line = None
        }
      }
    }

    /* Catch any remaining content in the case of documents that do not end in a complete sentence or 
     * a dropped line. Common in testing, kind of rare in the actual Common Crawl archive
     */
    if (builder.length != 0) {
      if (debug) println("The document did not end on a complete sentence.")
      // run the cleaner
      var cleaned: Option[String] = cleanTextblock(builder.toString())
      cleaned.foreach(tb => textblock.append(tb))
    }

	if (debug) println(hashes)
    var result_list: ListBuffer[String] = new ListBuffer[String]
    textblock.foreach { l => 
      result_list.append(l)
    }
    result_list.mkString("\n")
  }
}
