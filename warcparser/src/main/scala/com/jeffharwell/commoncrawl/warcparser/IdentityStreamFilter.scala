package com.jeffharwell.commoncrawl.warcparser

/**
 * Identity stream filter, mainly useful for testing. The identity stream filter will always return true
 * regardless of what document is passed to it.
 */
class IdentityStreamFilter extends WARCStreamFilter {

  /**
   * Always return true regardless of document passed.
   *
   * @param w a WARCRecord object
   * @tparam A <: WARCRecord
   * @return Boolean
   */
  def apply[A <: WARCRecord](w: A): Boolean = {
    true
  }
}
