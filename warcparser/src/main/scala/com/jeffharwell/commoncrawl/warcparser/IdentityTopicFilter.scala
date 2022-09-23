package com.jeffharwell.commoncrawl.warcparser

/*
 * This is a topic filter that categorizes each document
 * it sees as a document. This is handy if you want a
 * Parser that returns all of the documents that it parses.
 */
class IdentityTopicFilter() extends WARCTopicFilter {
  def hasCategories[A <: WARCRecord](w: A): Boolean = {
    true
  }
  def getCategories[A <: WARCRecord](w: A): Set[String] = {
    Set[String]("document")
  }
}
