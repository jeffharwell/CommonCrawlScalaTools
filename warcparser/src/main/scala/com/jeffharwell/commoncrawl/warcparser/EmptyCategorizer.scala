package com.jeffharwell.commoncrawl.warcparser

/*
 * This is a categorizer that never categorizes anything.
 * Handy if you want a Parser or WARCConversion object that
 * doesn't actually categorize anything.
 */
class EmptyCategorizer() extends WARCCategorizer {
  val emptyset = Set[String]()
  def hasCategories(s: String): Boolean = {
    false
  }
  def getCategories(s: String): Set[String] = {
    emptyset
  }
}

