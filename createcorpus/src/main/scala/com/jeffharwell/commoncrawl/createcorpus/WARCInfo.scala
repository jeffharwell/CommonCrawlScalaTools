package com.jeffharwell.commoncrawl.createcorpus

class WARCInfo() extends WARCRecord {
  override val requiredfields: List[String] = List[String](
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
}
