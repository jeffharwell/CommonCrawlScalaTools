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
  var alpha_pattern = Pattern.compile("[A-Za-z']+")
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

  def count_if_alpha(t: String): Int = {
    if (alpha_pattern.matcher(t).matches()) {
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

  def countAlpha(tokens: ListBuffer[String]): Int = {
    tokens.map(count_if_alpha).reduce(_ + _)
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
  // Basically this goes through the list of tokens and returns the index of the first token
  // that has a uppercase letter followed by a lower case letter. 
  def find_sentence_start_token_index(tokens: ListBuffer[String], previous_token: String = ""): Option[String] = {
    // why use a break if you can use tail recursion
    var h = tokens.head
    if (upper_pattern.matcher(h).matches()) {
      if (openingdoublequote_pattern.matcher(previous_token).matches()) {
        Some("\""+h)
      } else if (singlequote_pattern.matcher(previous_token).matches()) {
        Some("'"+h)
      } else {
        Some(h)
      }
    } else if (tokens.length > 1) {
      find_sentence_start_token_index(tokens.tail, h)
    } else {
      None
    }
  }

  def find_sentence_start_index(textblock: String, tokens: ListBuffer[String]): Option[Int] = {
    var token_string: Option[String] = find_sentence_start_token_index(tokens)  
    if (token_string.isDefined) {
      Some(textblock indexOf token_string.get)
    } else {
      None
    }
  }

  def reverseTail(l: ListBuffer[String]): ListBuffer[String] = {
    l.slice(0, l.length-1)
  }

  def find_sentence_end_token_index(tokens: ListBuffer[String], previous_token: String = ""): Option[Pattern] = {
    var t = tokens.last
    if (terminator_pattern.matcher(t).matches()) {
      if (closingdoublequote_pattern.matcher(previous_token).matches()) {
        // scala bug, escapes don't work in string interpolation :(
        Some(Pattern.compile("\\"+t+"\\s?\""))
      } else if (singlequote_pattern.matcher(previous_token).matches()) {
        Some(Pattern.compile("\\"+t+"\\s?'"))
      } else {
        // Every Possible Sentence Ending (!.?) will need to be escaped in the regular
        // expression
        Some(Pattern.compile("\\"+t))
      }
    } else if (tokens.length > 1) {
      find_sentence_end_token_index(reverseTail(tokens), t)
    } else {
      None
    }
  }

  def getIndexOfPattern(pattern: Pattern, textblock: String): Option[Int] = {
    var matcher = pattern.matcher(textblock)
    var end_index = -1

    // Go until we hit the last match, that should be the sentence terminator
    // get the index at the end of the match
    while (matcher.find()) {
      end_index = matcher.end()
    }
    if (end_index >= 0 ) {
      Some(end_index)
    } else {
      None
    }
  }

  def find_sentence_end_index(textblock: String, tokens: ListBuffer[String]): Option[Int] = {
    var token_pattern: Option[Pattern] = find_sentence_end_token_index(tokens)  
    if (token_pattern.isDefined) {
      if (debug) println(s"Searching Ending Pattern: ${token_pattern.get}")
      var end_of_sentence = getIndexOfPattern(token_pattern.get, textblock)
      end_of_sentence
    } else {
      // No sentence ending was found
      None
    }
  }

  def clean_textblock(textblock: String): Option[String] = {
    var tokens = tokenize_line(textblock)
    var start: Option[Int] = find_sentence_start_index(textblock, tokens)
    if (start.isDefined) {
      if (debug) println(s"Start index is: ${start}")
      var end: Option[Int] = find_sentence_end_index(textblock, tokens)
      if (end.isDefined && start.get < end.get) {
        if (debug) println(s"End index is: ${end}")
        Some(textblock.slice(start.get, end.get))
      } else {
        None
      }
    } else {
      None
    }
  }

  def keep_line(line: String): Boolean = {
    var tokens = tokenize_line(line)
    // hmm, this avoids computation until it is absolutely necessary
    // but it is certainly ugly to code. I guess it is a guard type
    // of pattern
    // This is from Laippala and Ginter 2014
    // They discarded lines that were 5 or less tokens, less than 70% alpha
    // (words, alphabetical is my interpretation of not "special character tokens")
    // or greater than 20% numeric
    if (tokens.size > 5) {
      var alpha_percent = BigDecimal(countAlpha(tokens))/tokens.size
      if (alpha_percent > .7) {
        var numeric_percent = BigDecimal(countNumeric(tokens))/tokens.size
        if (numeric_percent < .2) {
          true
        } else {
          false
        }
      } else {
        false
      }
    } else {
      false
    }
  }

  def detect_sentence_ending(line: String): Boolean = {
    var last = line.takeRight(1)
    var penultimate = line.takeRight(2).take(1)
    if (last == "." || last == "?" || last == "!") {
      true
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
    for (line <- document.split("\r?\n")) { // maybe a bit simplistic ... but it matches
                                            // matches the LineIterator class from Source.scala, although
                                            // using the much slower Regexp split.
                                            // https://github.com/scala/scala/blob/2.8.x/src/library/scala/io/Source.scala
      // First see if we are keeping the line
      if (keep_line(line)) {

        // Build the hash for the line, used later to detect highly duplicate document
        hashes += md5HashString(line)
        if (debug) println(line)
        if (builder.length == 0) {
          builder.append(line)
        } else {
          // Throw in a space if we are appending this to an existing line
          // often it seems like, in the WET archives, a new line is replacing
          // HTML code that probably generated a gap in the content
          builder.append(" "+line)
        }

        if (detect_sentence_ending(line)) {
          // We found the end of a sentenc, write out the text block
          // and start a new one
          if (debug) println("---Is sentence ending")
          var cleaned: Option[String] = clean_textblock(builder.toString())
          // the foreach will only run if there is a value in the option
          // Bascially, the potential text block might not even have enough info
          // in it to keep it, the cleaner might basically delete all the content.
          // https://danielwestheide.com/blog/2012/12/19/the-neophytes-guide-to-scala-part-5-the-option-type.html
          cleaned.foreach(tb => textblock.append(tb))
          builder = StringBuilder.newBuilder
        }
      } else {
        // We are dropping a line, the text block ends here
        if (builder.length > 0) {
          // if there is anything in the buffer
          var cleaned: Option[String] = clean_textblock(builder.toString())
          // the foreach will only run if there is a value in the option
          // Bascially, the potential text block might not even have enough info
          // in it to keep it, the cleaner might basically delete all the content.
          // https://danielwestheide.com/blog/2012/12/19/the-neophytes-guide-to-scala-part-5-the-option-type.html
          cleaned.foreach(tb => textblock.append(tb)) // the foreach will only run if there is a value
          builder = StringBuilder.newBuilder
        }
      }
    }

    /* Catch any remaining content in the case of documents that do not end in a complete sentence or 
     * a dropped line. Common in testing, kind of rare in the actual Common Crawl archive
     */
    if (builder.length != 0) {
      if (debug) println("The document did not end on a complete sentence.")
      // run the cleaner
      var cleaned: Option[String] = clean_textblock(builder.toString())
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
