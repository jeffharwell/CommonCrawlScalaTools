package com.jeffharwell.commoncrawl.createcorpus

/*
 * The WARC Categorizer Trait
 *
 * Defines an interface (Java speak) that must be implemented by all WARC Categorizers
 */
trait WARCCategorizer {
  def hasCategories(): Boolean

  def getCategories(): List[String]

  /*
   * This method operates on shared state (boo?) to categorize the string passed to it.
   * It will set hascategories = true if any categories are found. It will also set 
   * categories = List[String]('my','list','of','matching','categories')
   *
   * @param s The string of content to categorize
   * @return WARCCategorizer so that you can chain it c.categorize("mystring").getCategories.size
   */
  def categorize(s: String): WARCCategorizer
}
