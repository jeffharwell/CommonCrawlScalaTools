package com.jeffharwell.commoncrawl.warcparser

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

  // Setter for Minimum Mentions
  def setMinMentions(i :Int): Unit = {
    minimummentions = i
  }

  // Getter for Minimum Mentions
  def getMinMentions(): Int = {
    return(minimummentions)
  }

  /*
   * A sweet bit of Scala syntactic sugar
   * apply() is the function that gets called when an object is used 
   * as a function (not quite accurate but close). This this method 
   * gets called when:
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
    if (containsKeywords(w.fields("Content"), minimummentions)) { detailCheck(w) }
    else { false }
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

    // Single liner .. takes a string and first splits it by lines then by periods
    val chunks = wrecord.fields("Content").split("\\n").foldLeft(List[String]()) { (x, t) => x ++ t.split("\\. ") }

    // This is a bit tricky,
    // A recursive function that checks each chunk to see if it is 
    // < 50 "words" but greater than 7 "words". If so check containsKeyword. If 
    // true then we are done, it passes the filter, if false then grab the next chunk.
    def checkChunks(chunks: List[String]): Boolean = {
      val chunk = chunks.head
      val t = chunks.tail

      val chunksize = chunk.split(" ").size
      // It only has to
      if (chunksize > 7 && chunksize < 50 && containsKeywords(chunk, 1)) { true }
      else if (t.size == 0) { false }
      else { checkChunks(t) }
    }
    checkChunks(chunks)
  }

  def containsKeywords(tocheck: String, mentions: Int): Boolean = {
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

    if (length == 0)
        return src.size // a bit arbitrary here, you could say a String contains
                         // string.size empty strings ... or infinite .. whatever
                         // it just seems a bit punative to throw a runtime error

    val firstLo = Character.toLowerCase(what.charAt(0))
    val firstUp = Character.toUpperCase(what.charAt(0))
    //println(s"Initial match for ${firstLo} or ${firstUp}")

    for (i <- 0 to src.size - length) {
      val ch = src.charAt(i)
      //println(s"Looking at ${ch}")
      if (ch == firstLo || ch == firstUp) {
        //println("Got an initial match")
        if (src.regionMatches(true, i, what, 0, length)) {
          matches += 1
        }
      }
    }

    //println(s"Found ${matches} matches min mentions is ${minimummentions}")
    return matches
  }
}
