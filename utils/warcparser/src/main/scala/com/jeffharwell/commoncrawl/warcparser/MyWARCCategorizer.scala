package com.jeffharwell.commoncrawl.warcparser

import scala.collection.mutable.ListBuffer

/*
 * Jeff's WARC Categorizer
 *
 * At present this class applies some basic rules to the content of the WARC Record
 * and then allows you to either check for the existence of the categories or to return
 * a list of categories.
 *
 * This is not a general class. This is an implementation of my own categorizer requirements
 * The basic idea is that I am looking for keywords that, one, occur a certain number of
 * times and, two, occur in sentences of certain size.
 *
 * @param minimummentions The minimum number of times a keyword must appear before the 
 *                        content matches the category. This will default to 7.
 */
class MyWARCCategorizer(minimummentions: Int) extends WARCCategorizer {

  // Auxilary constructor that sets the minimum number of mentions to 7
  // if no minimum is specified.
  def this() = this(7)

  val keywords = Map[String, String](
    "trump" -> "politics",
    "clinton" -> "politics",
    "presidential" -> "politics",
    "asthma" -> "asthma"
  )

  /*
   * Returns a boolean if the string s matches any categories.
   *
   * @param s The string to categorize
   */
  def hasCategories(s: String): Boolean = {
    var categories = categorizeString(s, minimummentions, keywords)
    if (categories.size > 0) {
      true
    } else {
      false
    }
  }

  /*
   * Returns a set of categories that the string s matches
   *
   * @param s The string to categorize
   */
  def getCategories(s: String): Set[String] = {
    categorizeString(s, minimummentions, keywords).toSet
  }

  // Getter for Minimum Mentions
  def getMinMentions(): Int = {
    return(minimummentions)
  }

  /*
   * A more or less pure function, it takes as string as input and some matching 
   * parameters as input and returns a list of categories that match
   *
   * @param s The string of content to categorize
   * @param minnummentions The minimum number of mentions for a keyword needed for a match
   * @param keywords A Map() with the categories and their associated keywords
   * @return a list containing the categories that s fits into
   */
  def categorizeString(s: String, minnummentions: Int, keywords: Map[String,String]): List[String] = {
    var matchbuffer = ListBuffer[String]()
    // This is the first check
    keywords foreach { case(keyword, category) => 
      if (ciMatch(s, keyword) >= minimummentions ) {
        matchbuffer += category
      }
    }

    return matchbuffer.toList
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
