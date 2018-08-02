package com.jeffharwell.commoncrawl.warcparser

// Allows you to say var wi = WARCInfo() and get your object back
// this make it consistent with the usage of WARCConversion which has
// to use the helper object because it has multiple constructors.
object WARCInfo {
  def apply() = {
    new WARCInfo()
  }
}

class WARCInfo() extends WARCRecord {
  override val requiredfields: List[String] = List[String](
                                              "WARC-Type",
                                              "WARC-Date",
                                              "WARC-Filename",
                                              "WARC-Record-ID",
                                              "Content-Type",
                                              "Content-Length",
                                              "Content")

  /*
   * Return the size of the content of this record as an Integer number
   * if bytes.
   *
   * @return number of bytes of content as an integer
   */
  def getContentSizeInBytes(): Int = {
    if (!this.headersComplete()) {
      throw new RuntimeException("Cannot be sure of content length until all headers are required headers are added.")
    }
    return fields("Content-Length").toInt
  }

  /*
   * Add optional fields in case you want to add additional fields to 
   * this WARC record in addition to the required ones.
   *
   * @param m A map with the field and value to add
   */
  def addFields(m: scala.collection.immutable.Map[String, String]): Unit = {
    m.foreach { case (k, v) => 
      fields += k -> v
      // Check to make sure we are receiving headers for the right kind of record
      if (k == "WARC-Type" && v != "warcinfo") {
        throw new WARCRecordTypeException(s"Expecting WARC-Type = warcinfo but found ${v} instead")
      }
    }
  }

}
