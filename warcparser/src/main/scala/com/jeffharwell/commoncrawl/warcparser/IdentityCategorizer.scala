package com.jeffharwell.commoncrawl.warcparser

/*
 * This is a categorizer that simply categorizes documents
 * as documents. Handy if you want a parser that returns
 * all documents that it parses.
 */
class IdentityCategorizer() extends WARCCategorizer {
  def hasCategories(s: String): Boolean = {
    true
  }
  def getCategories(s: String): Set[String] = {
    Set[String]("document")
  }
}
