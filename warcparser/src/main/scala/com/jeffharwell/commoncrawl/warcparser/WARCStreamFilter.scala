package com.jeffharwell.commoncrawl.warcparser

/*
 * The WARC Categorizer Trait
 *
 * Defines an interface (Java speak) that must be implemented by all WARC Categorizers
 *
 * During the creation of the original political and health corpora (see dissertation, Harwell 2023) this
 * trait was call WARCCategorizer.
 */
trait WARCStreamFilter extends java.io.Serializable {
  /*
   * Most accept a WARCRecord as an argument an return a boolean to indicate
   * if the WARCRecord matches the filter criteria.
   *
   * @param w The WARCRecord to evaluate
   */
  def apply[A <: WARCRecord](w: A): Boolean
}
