package com.jeffharwell.commoncrawl.createcorpus

/*
 * This is a categorizer that never categorizes anything.
 * Handy if you want a Parser or WARCConversion object that
 * doesn't actually categorize anything.
 */
class EmptyCategorizer() extends WARCCategorizer {
  val emptyset = Set[String]()
  def hasCategories(): Boolean = {
    false
  }
  def getCategories(): Set[String] = {
    emptyset
  }
  def categorize(s: String): WARCCategorizer = {
    // doesn't do anything and returns itself
    // remember that Scala the last expression is taken 
    // as the value that is returned.
    // https://tpolecat.github.io/2014/05/09/return.html
    this
  }
}

