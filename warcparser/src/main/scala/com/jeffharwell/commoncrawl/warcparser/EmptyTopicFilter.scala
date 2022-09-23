package com.jeffharwell.commoncrawl.warcparser

/*
 * This is a categorizer that never categorizes anything.
 * Handy if you want a Parser or WARCConversion object that
 * doesn't actually categorize anything.
 */
class EmptyTopicFilter() extends WARCTopicFilter {
  val emptyset = Set[String]()
  def hasCategories[A <: WARCRecord](w: A): Boolean = {
    false
  }
  def getCategories[A <: WARCRecord](w: A): Set[String] = {
    emptyset
  }
}

