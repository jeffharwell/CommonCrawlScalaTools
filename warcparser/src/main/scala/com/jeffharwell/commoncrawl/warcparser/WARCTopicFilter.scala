package com.jeffharwell.commoncrawl.warcparser

/*
 * The WARC Categorizer Trait
 *
 * Defines an interface (Java speak) that must be implemented by all WARC Categorizers
 *
 * During the creation of the original political and health corpora (see dissertation, Harwell 2023) this
 * trait was call WARCCategorizer.
 */
trait WARCTopicFilter extends java.io.Serializable {
  /*
   * This method returns true or false depending on if the string s
   * is categorized by the categorizer.
   *
   * @param s The string of content to categorize
   */
  def hasCategories[A <: WARCRecord](w: A): Boolean

  /*
   * This method returns a set containing each category that the string s
   * matches
   *
   * @param s The string of content to categorize
   */
  def getCategories[A <: WARCRecord](w: A): Set[String]
}
