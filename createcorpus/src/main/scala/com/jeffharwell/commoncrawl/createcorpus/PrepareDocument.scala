package com.jeffharwell.commoncrawl.createcorpus

import scala.collection.mutable.ListBuffer
import java.io.StringReader
import java.util.regex.Pattern
import java.security.MessageDigest
import java.math.BigInteger
import scala.annotation.tailrec
import scala.collection.mutable
import edu.stanford.nlp.process.PTBTokenizer
import edu.stanford.nlp.process.CoreLabelTokenFactory


/*
 * This class attempts to implement the Common Crawl document cleanup strategy
 * outline by:
 * Laippala, V., & Ginter, F. (2014). Syntactic n-gram collection from a large-scale corpus of internet finnish. In IOS Press (Vol. 268, p. 184).
 */

class PrepareDocument(document: String) extends java.io.Serializable {

  /*
   * Deal with counting and classifying tokens
   */
  var alpha_num_pattern: Pattern = Pattern.compile("[A-Za-z0-9'`]+")
  var upper_pattern: Pattern = Pattern.compile("^[A-Z].*")
  var terminator_pattern: Pattern = Pattern.compile("[.!?][\"']*$")
  var openingdoublequote_pattern: Pattern = Pattern.compile("``") // Tokenizer converts opening " to ``
  var closingdoublequote_pattern: Pattern = Pattern.compile("''") // Tokenizer converts closing " to ''
  var singlequote_pattern: Pattern = Pattern.compile("'") // Tokenizer leaves single quotes alone
  var numeric_pattern: Pattern = Pattern.compile("[0-9]+")
  var max_tokens_before_sentence_end = 70
  var debug = false

  def setDebug(): Unit = {
    debug = true
  }

  def count_if_alpha_num(t: String): Int = {
    if (alpha_num_pattern.matcher(t).find()) {
      1
    } else {
      0
    }
  }
  def count_if_numeric(t: String): Int = {
    if (numeric_pattern.matcher(t).find()) {
      1
    } else {
      0
    }
  }

  def countAlphaNum(tokens: ListBuffer[String]): Int = {
    tokens.map(count_if_alpha_num).sum
  }

  def countNumeric(tokens: ListBuffer[String]): Int = {
    tokens.map(count_if_numeric).sum
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
    val line_as_reader = new StringReader(line)
    val ptbt = new PTBTokenizer(line_as_reader, new CoreLabelTokenFactory(), "")
    val tokens = new ListBuffer[String]()
    while (ptbt.hasNext) {
      val label = ptbt.next()
      if (debug) println(label)
      tokens += label.originalText() //edu.stanford.nlp.ling.CoreLabel
    }
    tokens
  }

  /*
   * We want allow sentences to start with something like "( or "[. These are converted
   * into tokens ['"', "-LRB-"] by the PTBTokenizer. Need to handle that in the
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

  var valid_opening_brackets: List[String] = List[String]("-LRB-","-LSB-","-LCB-")
  var valid_quotes: List[String] = List[String]("`","'","''","``")

  @scala.annotation.tailrec
  final def getSentenceAdditionalStartCharacters(previous_tokens: List[String], sentence_start: String = ""): Option[String] = {
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
    if (previous_tokens.isEmpty) {
      // well, that was easy
      None
    } else {
      val h = previous_tokens.head
      if ((valid_opening_brackets contains h) && (sentence_start == "")) { // we only accept an opening bracket as the first character before the capitalized token
        // we have a bracket
        val translated: String = translateTreebankToken(h)
        // and recurse
        getSentenceAdditionalStartCharacters(previous_tokens.tail, translated + sentence_start)
      } else if (valid_quotes contains h) {
        // we have a quote
        val translated: String = translateTreebankToken(h)
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

  @scala.annotation.tailrec
  final def findSentenceStartIndex(textblock: String, tokens: ListBuffer[String], previous_tokens: List[String] = List[String]()): Option[Int] = {
    val h = tokens.head
    if (upper_pattern.matcher(h).find()) {
      // alright, we have the token that starts the sentence, now what else could be in front that could
      // also be part of the sentence?
      val other_characters: Option[String] = getSentenceAdditionalStartCharacters(previous_tokens)
      if (other_characters.isDefined) {
        // Trickier, we have tokens that might be part of the start of the sentence. For example the 
        // string "[Bob] did the thing!" previous = "[ and h = Bob. getStartIndexWithAdditionalCharacters
        // handles the case where the text block actually has the value " [ Bob] did the thing!" and we need
        // to find the index of the opening double quotes.
        getStartIndexWithAdditionalStartCharacters(textblock, other_characters.get, h)
      } else {
        // Easy, there are no valid tokens before the uppercase token that we just matched
        // which might be part of the sentence. So get the index of the token we matched 
        // and just return it.
        Some(textblock indexOf h)
      }
    } else if (tokens.length > 1) { // we did not find an token that starts with a capital letter, but have some tokens left, keep looking
      val new_previous: List[String] = h :: previous_tokens // adds the current head token to the front of the previous tokens list
      findSentenceStartIndex(textblock, tokens.tail, new_previous)
    } else {
      None
    }
  }

  // We have a token that starts the sentence in the text block, say the token 'Bob'. We also have some characters that go before it but belong
  // to the same sentence, say the string '"['. Now, the trick is that the PTB Tokenizer drops spaces as it tokenizes, so the textblock might
  // actually be: 'end of a fragment. " [ Bob] did the thing!"', and we need to find the index of the opening double quote. I'm going to try to
  // just hack this thing together with recursion rather then do a full FSA which would solve this elegantly ... blargh
  @scala.annotation.tailrec
  final def getStartIndexWithAdditionalStartCharacters(textblock: String, previous_characters: String, sentence_start_token: String, search_index: Int = 0, textblock_index: Int = 0, current_match: Int = -1): Option[Int] = {
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
    val valid_additional_ending_characters = List('\u201d','\u2019','"','\'','\u2019',']',')')
    val search_index = ending_index + 1

    @tailrec
    def findNewIndex(ending_index: Int, search_index: Int): Int = {
      if (search_index >= textblock.length) {
        // We just overran the end of the string, return whatever we may have found
        ending_index
      } else if (valid_additional_ending_characters.contains(textblock(search_index))) {
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

  // Pretty Simple, if the string s ends with any of the invalid_endings in the ListBuffer
  // it will return true, false otherwise
  // Basically when the PTB tokenizer works through a sentence it will catch things like
  // Mr. Ms. Jr. Rev. and other common abbreviations and consider them a distinct token,
  // so if you pass all of the tokens that end in a period to this function as invalid_endings
  // it will tell you if your current string ends with one of those endings
  @scala.annotation.tailrec
  final def endsWithInvalid(s: String, invalid_endings: ListBuffer[String]): Boolean = {
    if (invalid_endings.isEmpty) {
      false
    } else {
      val ending = invalid_endings.head
      if (s.endsWith(ending)) {
        true
      } else {
        endsWithInvalid(s, invalid_endings.tail)
      }
    }
  }

  // Little function which evaluates the text block at the current index to see if the current index
  // represents a valid sentence ending. It much be passed a list buffer which contains all tokens 
  // that end with a period but do not constitute a sentence ending.
  // This also takes care of cases where you have multiple periods in a row or cases where there is a 
  // single letter and a period (such as a first initial) which shouldn't be counted.
  def isValidSentenceEnding(textblock: String, current_index: Int, invalid_endings: ListBuffer[String]): Boolean = {
    val valid_ending_characters = "?!"
    //val invalid_endings = List("mr","ms","sr","jr")
    if (valid_ending_characters contains textblock(current_index)) {
      true  
    } else if (textblock(current_index) == '.') {
      if (hasPeriodPrevious(textblock, current_index) || hasPeriodNext(textblock, current_index)) {
        false
      } else if (endsWithInvalid(textblock.slice(0, current_index + 1), invalid_endings)) {
        false
      } else {
        true
      }
    } else {
      false
    }
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

  // if the next character is either a space or the end of the string return true,
  // false otherwise
  def hasSpaceOrEndingNext(textblock: String, current_index: Int): Boolean = {
    if (current_index + 1 == textblock.length) {
      true
    } else if (textblock(current_index + 1) == ' ') {
      true
    } else {
      false
    }
  }


  // Look through the textblock backwards, character by character, and return the index that marks the end
  // of the last sentence in the textblock..
  def findSentenceEndIndex(textblock: String, tokens: ListBuffer[String]): Option[Int] = {
    if (textblock.isEmpty) {
      None
    }
    def ends_with_period(x: String): Boolean = {
      if (x.length > 1 && x(x.length - 1) == '.') {
        true
      } else {
        false
      }
    }
    val tokenizer_invalid_endings = tokens.filter(ends_with_period)
    val other_invalid_endings = List("Ret.")
    val invalid_endings = tokenizer_invalid_endings ++ other_invalid_endings

    @tailrec
    def findIndex(textblock: String, index: Int): Option[Int] = {
      if (index == 0) {
        // we went all the way through the sentence and didn't find anything
        None
      //} else if (textblock(index) == '.' || textblock(index) == '?' || textblock(index) == '!') {
      } else if (isValidSentenceEnding(textblock, index, invalid_endings)) {
        // We have a sentence ending, see if there are any additional characters to tack onto
        // sentence (quotes, etc.)
        val new_index = adjustIndexForAdditionalCharacters(textblock, index)
        if (new_index == index && !hasSpaceOrEndingNext(textblock, index)) {
          if (debug) println(s"Rejected a sentence ending at index = $index because it did not have a space, line ending, or valid additional characters after it")
          // there are no additional charcters, to account for the fact that this
          // doesn't actually seem to be the end of a sentence, reject it, and keep going
          findIndex(textblock, index - 1)
        } else {
          // Ok, the lack of space or newline is caused by valid additional characters.
          Some(new_index)
        }
      } else {
        // we found nothing, but we are not at the end yet, recurse
        findIndex(textblock, index - 1)
      }
    }

    findIndex(textblock, textblock.length - 1)
  }

  def findIndexOfFirstSentenceEndingToken(tokens: ListBuffer[String]): Option[Int] = {
    val sentence_ending_tokens = List(".","!","?")

    val earliest_ending_token = sentence_ending_tokens.map( x => tokens.indexOf(x)).filter(_ != -1)
    if (earliest_ending_token.nonEmpty) {
      if (debug) println(s"Earliest ending token is: $earliest_ending_token")
      Some(earliest_ending_token.min)
    } else {
      None
    }
  }


  // Clean the text block by removing anything from the beginning that does not belong to a sentence
  // (as best we can tell) and anything from the end that does not belong to a sentence (again, as best
  // we can tell ... a lot of this stuff isn't in complete sentences anyways.)
  def cleanTextBlock(textblock: String): Option[String] = {
    val tokens = tokenize_line(textblock)
    val start: Option[Int] = findSentenceStartIndex(textblock, tokens)
    val earliest_ending: Option[Int] = findIndexOfFirstSentenceEndingToken(tokens)
    if (start.isDefined && earliest_ending.isDefined && earliest_ending.get < max_tokens_before_sentence_end) {
      if (debug) println(s"Start index is: $start")
      val end: Option[Int] = findSentenceEndIndex(textblock, tokens)
      if (end.isDefined && start.get <= end.get) {
        if (debug) println(s"End index is: $end")
        Some(textblock.slice(start.get, end.get + 1))
      } else {
        if (debug && end.isDefined) println(s"Found end $end but it is less than start $start, discarding block.")
        if (debug && end.isEmpty) println("Did not found a valid sentence ending. Discarding block.")
        None
      }
    } else {
      if (earliest_ending.isEmpty) {
        if (debug) println(s"No sentence endings at all were found in the textblock, discarding.")
      } else if (earliest_ending.get > max_tokens_before_sentence_end) {
        if (debug) println(s"Rejected textblock because the earlist end token occurred at token ${earliest_ending.get}, which is larger than the the maximum tokens that can occur before a sentence ending, $max_tokens_before_sentence_end")
      }
      if (start.isEmpty) {
        if (debug) println("Rejected textblock because no valid sentence starting sequence was found")
      }
      None
    }
  }

  def keepLine(line: String, in_textblock: Boolean): Boolean = {
    val tokens = tokenize_line(line)
    /* hmm, this avoids computation until it is absolutely necessary
     * but it is certainly ugly to code. I guess it is a guard type
     * of pattern.
     *
     * This is from Kanerva et al. 2014
     * They discarded lines that were 5 or less tokens, less than 70% alpha
     * (words, alphabetical is my interpretation of not "special character tokens")
     * or greater than 20% numeric
     */
    if (debug) println("Evaluating Line:")
    if (debug) println(line)
    if (in_textblock && detect_sentence_ending(line)) {
      // we are currently in a text block and this looks like the end of a sentence
      // we don't care how short it is, go ahead and keep it
      // I added this heuristic because, at least in the documents I tested, it seems that
      // often a text block's last sentence will end with just a word or two on the last line.
      // Without this rule the cleaner will discard the entire text block.
      if (debug) println("    Accepting line because it looks like the end of the sentence for the current text block.")
      true
    } else if (tokens.size > 5) {
      val alpha_percent = BigDecimal(countAlphaNum(tokens))/tokens.size
      if (alpha_percent > .65) {
        val numeric_percent = BigDecimal(countNumeric(tokens))/tokens.size
        if (numeric_percent < .3) {
          if (debug) println("    Accepted line")
          true
        } else {
          if (debug) println(s"    Rejected line: it did not have less than 20% numeric characters. Percent was $numeric_percent")
          false
        }
      } else {
        if (debug) println(s"    Rejected line: it did not have more than 65% alpha numeric characters. Percent was $alpha_percent")
        false
      }
    } else {
      if (debug) println("    Rejected line: it did not have > 5 tokens")
      false
    }
  }

  def detect_sentence_ending(line: String): Boolean = {
    val last = line.takeRight(1)
    val penultimate = line.takeRight(2).take(1)
    //var antipenultimate = line.takeRight(3).take(1)
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

  // This is basically a very low bar for keeping a line
  // it has to have two tokens to rub together, and at
  // least one of them has to be alphanumeric.
  def keepMiddleLine(line: String): Boolean = {
    val tokens = tokenize_line(line)
    if (debug) println(s"   Number of tokens is ${tokens.length}")
    if (debug) println(s"   Alpha numeric tokens is ${countAlphaNum(tokens)}")
    if (tokens.length >= 2 && countAlphaNum(tokens) >= 1) {
      if (debug) println("   Keeping this middle line")
      true
    } else {
      if (debug) println("   Rejecting this middle line")
      false
    }
  }

  def prepare(): String = {
    // See the API Usage Section of
    // https://nlp.stanford.edu/software/tokenizer.shtml

	  val hashes: ListBuffer[String] = new ListBuffer()
    var builder = mutable.StringBuilder.newBuilder
    val textblocks: ListBuffer[String] = ListBuffer()
    var in_textblock: Boolean = false
    var last_dropped_line: Option[String] = None
    var potential_middle_line: Option[String] = None

    // a little helper function.
    // This is a bit subtle, if the last line that we dropped looks like it could be the start
    // of a text block (i.e. it begins with a capital letter and the actual text block does not)
    // then throw it onto the front of the current textblock we are evaluating. Otherwise just
    // include all of the non-dropped lines which are in the builder.
    def buildTextBlock(last_dropped_line: Option[String], string_builder: mutable.StringBuilder): String = {
      //if (debug) println("Looking at:--"+string_builder.toString()+"--")
      if (last_dropped_line.isDefined && upper_pattern.matcher(last_dropped_line.get).find() && !upper_pattern.matcher(string_builder.toString()).find()) {
        if (debug) println("Including last dropped line")
        if (debug) println("    "+last_dropped_line.get)
        last_dropped_line.get + "\n" + builder.toString()
      } else {
        builder.toString()
      }
    }
    // Check to see how many "tokens" are in a string
    def approximateTokenCount(s: String): Int = {
      s.split("\\s+").length
    }
    // Append a string to the textblock
    def appendToTextblock(tb: ListBuffer[String], line: String): Unit = {
      if (approximateTokenCount(line) > 4) {
        if (debug) println("Line/textblock has more than 4 tokens, including it")
        tb.append(line)
      } else {
        if (debug) println("Line/textblock has 4 or less tokens, discarding")
      }
    }


    for (line <- document.split("\r?\n")) { // maybe a bit simplistic ... but it matches
                                            // matches the LineIterator class from Source.scala, although
                                            // using the much slower Regexp split.
                                            // https://github.com/scala/scala/blob/2.8.x/src/library/scala/io/Source.scala
      // First see if we are keeping the line
      if (keepLine(line, in_textblock)) {
        // Build the hash for the line, used later to detect highly duplicate document
        hashes += md5HashString(line)
        if (builder.isEmpty) {
          builder.append(line)
          in_textblock = true
        } else {
          // Throw in a space if we are appending this to an existing line
          // often it seems like, in the WET archives, a new line is replacing
          // HTML code that probably generated a gap in the content
          if (potential_middle_line.isDefined) {
            builder.append("\n"+potential_middle_line.get)
            potential_middle_line = None
          }
          builder.append("\n"+line)
        }

        if (detect_sentence_ending(line)) {
          // We found the end of a sentence, write out the text block
          // and start a new one
          if (debug) println("---Is sentence ending")
          val cleaned: Option[String] = cleanTextBlock(buildTextBlock(last_dropped_line, builder))
          // the foreach will only run if there is a value in the option
          // Basically, the potential text block might not even have enough info
          // in it to keep it, the cleaner might basically delete all the content.
          // https://danielwestheide.com/blog/2012/12/19/the-neophytes-guide-to-scala-part-5-the-option-type.html
          cleaned.foreach(tb => appendToTextblock(textblocks, tb))
          if (debug && cleaned.isEmpty) println("*** No valid sentences found in text block, discarding")
          if (debug) println("*** New Text Block ***")
          builder = mutable.StringBuilder.newBuilder
          in_textblock = false
          // Since we just processed a text block without dropping a line go ahead and clear
          // the last_dropped_line since we know it will not apply to the next text block
          // we are starting on.
          last_dropped_line = None
        }
      } else {
        // We may be dropping a line, if we do then the text block ends here, but under some circumstances we won't
        // be sure until the next line
        if (builder.nonEmpty && potential_middle_line.isDefined) {
          // if there is anything in the buffer and we have already had one line that we thought about dropping
          // then we drop both lines and process the text block (last_dropped_line is a line that might belong on
          // the front of the text block, it has nothing to do with the interstitial line evaluation)
          if (debug) println("We dropped two lines, ending the text block, processing remaining text in the string builder")
          potential_middle_line = None
          val cleaned: Option[String] = cleanTextBlock(buildTextBlock(last_dropped_line, builder))
          if (debug) {
            if (cleaned.isDefined) {
              println("Cleaner returned content, whatever remained in the string builder did not contain a complete sentence.")
            } else {
              println("Cleaner did not return content")
            }
          }
          // the foreach will only run if there is a value in the option
          // Basically, the potential text block might not even have enough info
          // in it to keep it, the cleaner might basically delete all the content.
          // https://danielwestheide.com/blog/2012/12/19/the-neophytes-guide-to-scala-part-5-the-option-type.html
          //cleaned.foreach(tb => textblock.append(tb)) // the foreach will only run if there is a value
          cleaned.foreach(tb => appendToTextblock(textblocks, tb))
          if (debug && cleaned.isEmpty) println("*** No valid sentences found in text block, discarding")
          if (debug) println("*** New Text Block ***")
          builder = mutable.StringBuilder.newBuilder
          in_textblock = false
        } else if (builder.nonEmpty && keepMiddleLine(line)) {
          // the line seems to be in the middle of two valid lines and it has
          // at least two tokens and some letters
          // Keep it and if the next line is valid we will include it and keep
          // trucking on the text block
          if (debug) println("  Keeping potential interstitial line")
          potential_middle_line = Some(line)
        } else if (builder.nonEmpty) {
          // We will NOT be keeping this as a potential interstitial line (that case is caught in the previous condition)
          // so this is the end of the text block. Go ahead and build and clean it.
          if (debug) println("We have rejected a potential interstitial line, go ahead and build and clean the text block")
          val cleaned: Option[String] = cleanTextBlock(buildTextBlock(last_dropped_line, builder))
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
          cleaned.foreach(tb => appendToTextblock(textblocks, tb)) // the foreach will only run if there is a value

          // Reset all the things
          if (debug && cleaned.isEmpty) println("*** No valid sentences found in text block, discarding")
          if (debug) println("*** New Textblock ***")
          builder = mutable.StringBuilder.newBuilder
          in_textblock = false
          potential_middle_line = None
        }

        // We are dropping this line. Let's see if we might want to keep it as the start of the
        // next text block
        if (potential_middle_line.isEmpty) {
          in_textblock = false
          if (upper_pattern.matcher(line).find()) {
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
    }

    /* Catch any remaining content in the case of documents that do not end in a complete sentence or
     * a dropped line. Common in testing, kind of rare in the actual Common Crawl archive
     */
    if (builder.nonEmpty) {
      if (debug) println("The document did not end on a complete sentence.")
      // run the cleaner
      val cleaned: Option[String] = cleanTextBlock(builder.toString())
      cleaned.foreach(tb => appendToTextblock(textblocks, tb))
    }

	if (debug) println(hashes)
    val result_list: ListBuffer[String] = new ListBuffer[String]
    textblocks.foreach { l =>
      result_list.append(l)
    }
    result_list.mkString("\n")
  }
}
