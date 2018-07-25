package com.jeffharwell.commoncrawl.createcorpus

/*
 * The WARC Categorizer Trait
 *
 * Defines an interface (Java speak) that must be implemented by all WARC Categorizers
 */
trait WARCCategorizer extends java.io.Serializable {
  /*
   * This method returns true or false depending on if the string s
   * is categorized by the categorizer.
   *
   * @param s The string of content to categorize
   */
  def hasCategories(s: String): Boolean

  /*
   * This method returns a set containing each category that the string s
   * matches
   *
   * @param s The string of content to categorize
   */
  def getCategories(s: String): Set[String]
}
