package com.jeffharwell.commoncrawl.warcparser

import scala.annotation.tailrec
import scala.collection.mutable.ListBuffer

/*
 * Call the implements a WARC Record Filter
 *
 * At present this class applies some basic rules to the content of the WARC Record
 * and returns true if it matches false otherwise.
 */
class MyWARCFilter() extends java.io.Serializable {
  /*
   * MyWARCFilter
   *
   * This is not a general class. This is an implementation of my own filter requirements
   * The basic idea is that I am looking for keywords that, one, occur a certain number of
   * times and, two, occur in sentences of certain size.
   *
   * This filter is just the first cut, sentence parsing and relational tuple extraction 
   * come next, so it is more important that this filter be fast than that it be accurate.
   * I expect it to need to filter around 1 billion documents and extract the .1% that are
   * actually interesting for my research.
   */
  val keywords = List[String]("trump","clinton","presidential","asthma")

  // controls how many times a keyword must be mentioned in the 
  // content before it is considered a match. Defaults to 7.
  var minimummentions = 7

  var debug = false
  var debug_message_length = 500000
  // this contols the maximum number of chunks that the detailed test with check for
  // the presence of keywords.
  var max_chunks_to_check = 5000

  // Setter for Minimum Mentions
  def setMinMentions(i :Int): Unit = {
    minimummentions = i
  }

  // Getter for Minimum Mentions
  def getMinMentions(): Int = {
    return(minimummentions)
  }

  /*
   * Sets the debug flag. This will result in the code writing debug messages
   * to the standard output. The debug messages are designed to help with 
   * performance profiling
   */
  def setDebug() {
    debug = true
    println("Debugging is on for MyWARCFilter")
    println(s"  Debugging output for records where length > ${debug_message_length}")
    println(s"  Will only check a maximum of ${max_chunks_to_check} chunks when doing a detailed check.")
  }

  /*
   * Sets the message length at which debugging messages will be written 
   * when setDebug has been called
   */
  def setDebugMessageLength(l: Int): Unit = {
    debug_message_length = l
  }

  /*
   * The filter will only check a certain number of chunks when doing a detailed check. Once you get
   * over 100,000 it can take several minutes to do the detailed check. The code defaults to 40,000 
   * which can take over a minute to process on my laptop.
   * It also indirectly controls some debugging output. The debug
   * output from the detailed check will not print if the number of chunks is less than this 
   * threshold.
   */
  def setMaxChunksToCheck(m: Int): Unit = {
    max_chunks_to_check = m
  }

  /*
   * A sweet bit of Scala syntactic sugar
   * apply() is the function that gets called when an object is used 
   * as a function (not quite accurate but close). This method is 
   * called when:
   *
   * val myfilter = new MyWARCFilter()
   * myfilter(mywarcrecord)
   *
   * See: http://stackoverflow.com/questions/9737352/what-is-the-apply-function-in-scala
   *
   * [A <: WARCRecord] means that this method requires a subtype of WARCRecord as an argument
   *
   * @param w a WARCRecord to filter
   * @return a boolean, either the record passed the filter or doesn't
   */
  def apply[A <: WARCRecord](w: A): Boolean = {
    // containsKeywords is the simple check, just does a fast pattern match for each 
    // keyword in the content and returns true if any keyword passes a minimum number 
    // of mentions
    //
    // detailCheck is the more expensive check 
    if (debug && w.fields("Content").length > debug_message_length) { println(s"Processing Record ${w.get("WARC-Record-ID")} of length ${w.fields("Content").length}") }
    var start_time = System.currentTimeMillis()
    if (containsKeywords(w.fields("Content"), minimummentions)) { 
      var end_time = System.currentTimeMillis()
      if (debug && w.fields("Content").length > debug_message_length) { println(s"Running containsKeywords took ${end_time - start_time} ms") }
      detailCheck(w)
    } else { 
      false
    }
  }

  def detailCheck[A <: WARCRecord](wrecord: A): Boolean = {
    // Now the more extensive check, if anything matched split the content
    // into lines and make sure there is at least one match on a line where
    // the wordcount is greater than 7 (I just made that number up) but less than 50
    // (made that up too) because I want to filter out spam keyword stuffing.
    //
    // I could probably do something clever like have the ciMatch also return
    // the indicies of the match in the content, then I split the content by ". " look at
    // the starting and ending indices (have to screw with it a bit to account for 
    // the missing ". ") and then count the words for the matching strings

    // Hmm, lets try this first
    // Split content by newlines into chunks
    // Split chunks by spaces into "words"
    // Throw out the chunks that don't match our criteria
    // Run cimatch over the remaining chunks

    var start_time = System.currentTimeMillis()
    /* The functional one liner, it is A LOT slower than the imperative version below with uses the list buffer
     * 33236 ms to generate 46,000 chunks vs 39 ms for the imperative version 
     *
    val chunks = wrecord.fields("Content").split("\\n").foldLeft(List[String]()) { (x, t) => x ++ t.split("\\. ") }
    */
    /*
     * The imperative version
     */
    val newline_chunks: Array[String] = wrecord.fields("Content").split("\\n")
    val chunks: ListBuffer[String] = new ListBuffer()
    newline_chunks.foreach { x => 
      chunks.appendAll(x.split("\\. "))
    }

    var end_time = System.currentTimeMillis()

    if (debug && chunks.length > max_chunks_to_check) { println(s"Generated ${chunks.length} chunks in ${end_time - start_time} ms") }
    
    // checkChunks is expensive and will run for a long time if you process tens of thousands or hundreds of thousands 
    // of chunks, especially of those chunks are small.
    //
    // We only want to do a detailed check on things that might actually be sentences. Since we split by newline and then periods it 
    // seems reasonable to say that we one only interested in chunks that have between 7 and 50 words.
    def chunkFilter(chunk: String): Boolean = {
      val chunksize = chunk.split(" ").filter { y => y != "" }.length // filter, otherwise each empty string counts in the length, not what we want
      if (chunksize > 7 && chunksize < 50)  {
        true
      } else {
        false
      }
    }
    val valid_chunks = chunks.filter(chunkFilter(_))

    // We might still get a really large number of chunks to check, we are only going to process the first X thousand
    // We don't want to run for more than a few minutes or it might cause problems, especially if this is being used 
    // as the filter on a network stream .. things will start timing out.
    if (debug && chunks.length > max_chunks_to_check) { println(s"Found ${valid_chunks.length} chunks of appropriate size to check") }

    // Calculate the chunk number to which we will process before quiting based on the max_chunks_to_check
    var min = {
      if (valid_chunks.length > max_chunks_to_check) {
        valid_chunks.length - max_chunks_to_check 
      } else {
        0
      }
    }
    if (debug && valid_chunks.length > max_chunks_to_check) { println(s"Only searching to depth ${min}") }

    @tailrec
    def checkChunks(chunks: ListBuffer[String], min: Int): Boolean = {
      val chunk = chunks.head
      val t = chunks.tail
      val t_size = t.size

      if (t_size <= min) { false }
      else if (t_size == 0) { false }
      else if (containsKeywords(chunk, 1)) { true }
      else { checkChunks(t, min) }
    }
    start_time = System.currentTimeMillis()
    // don't pass checkChunks an empty list, it bombs out on chunks.head
    var has_mentions: Boolean = { if (valid_chunks.length == 0) { false }
                                  else { checkChunks(valid_chunks, min) }
                                }
    end_time = System.currentTimeMillis()
    if (debug && chunks.length > max_chunks_to_check) { println(s"checkChunks ran in ${end_time - start_time} ms") }
    has_mentions
  }

  def containsKeywords(tocheck: String, mentions: Int): Boolean = {
    @tailrec
    def checkKeyword(tocheck: String, keywordlist: List[String]): Boolean = {
      val keyword = keywordlist.head
      val t = keywordlist.tail
      
      if (ciMatch(tocheck, keyword) >= mentions) { true }
      else if (t.size == 0) { false }
      else { checkKeyword(tocheck, t) }
    }
    checkKeyword(tocheck, keywords)
  }

  /* ciMatch
   * 
   * This is a psudo optimized implementation of a caseInsensitive matching algorithm
   * Converted from some plain old nasty Java derived from @icza 's answer to 
   * http://stackoverflow.com/questions/86780/how-to-check-if-a-string-contains-another-string-in-a-case-insensitive-manner-in
   *
   * @param src the string to search for a match
   * @param what the string to match
   * @returns int the number of matches
   */
  def ciMatch(src: String, what: String): Int = {
    val length: Int = what.size
    var matches = 0
    var lowerwhat = what.toLowerCase()

    if (length == 0)
        return src.size // a bit arbitrary here, you could say a String contains
                         // string.size empty strings ... or infinite .. whatever
                         // it just seems a bit punative to throw a runtime error

    val firstLo = Character.toLowerCase(what.charAt(0))
    val firstUp = Character.toUpperCase(what.charAt(0))

    for (i <- 0 to src.size - length) {
      val ch = src.charAt(i)
      if (ch == firstLo || ch == firstUp) {
        /*
        if (lowerwhat == src.slice(i, i+length).toLowerCase()) {
          matches += 1
        }
        */
        // regionMatches runs one or two seconds faster overall on a WARC file than the src.slice version above
        if (src.regionMatches(true, i, what, 0, length)) {
          matches += 1
        }
      }
    }

    return matches
  }
}
