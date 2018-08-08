package com.jeffharwell.commoncrawl.warcparser

/*
 * The WARC Categorizer Trait
 *
 * Defines an interface (Java speak) that must be implemented by all WARC Categorizers
 */
trait ParserTrigger extends java.io.Serializable {
  /*
   * This method is the one that will be called by the parser so it must
   * be implemented.
   *
   * @param s The string of content to categorize
   */
  def call(s: String): Unit
}
