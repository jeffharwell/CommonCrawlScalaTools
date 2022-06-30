package com.jeffharwell.commoncrawl.warcparser

/*
 * This is a topic filter that categorizes each document
 * it sees as a document. This is handy if you want a
 * Parser that returns all of the documents that it parses.
 */
class IdentityTopicFilter() extends WARCCategorizer {
  def hasCategories(s: String): Boolean = {
    true
  }
  def getCategories(s: String): Set[String] = {
    Set[String]("document")
  }
}
