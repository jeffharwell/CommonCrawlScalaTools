package com.jeffharwell.commoncrawl.createcorpus

class WARCConversion() extends WARCRecord {
  override val requiredfields: List[String] = List[String](
                                              "WARC-Target-URI",
                                              "WARC-Date",
                                              "WARC-Record-ID",
                                              "WARC-Refers-To",
                                              "WARC-Block-Digest",
                                              "Content-Type",
                                              "Content-Length",
                                              "Content")

  var requiredwarcinfo:Option[WARCInfo] = None

  /*
   * Set the WARCInfo object that gives the context for this WARCConversion object
   *
   * There is probably a much better implementation than to,
   *
   * @param w WARCInfo object
   */
  def addWARCInfo(w: WARCInfo): Unit = {
    requiredwarcinfo = Some(w)
  }

  /*
   * Returns true if there is a value for each required field and each
   * additional required field added after initialization. False otherwise.
   * In the case of a WARCConversion object we also require that a warcinfo
   * object be present so that we can figure out what archive file this 
   * thing came from.
   *
   * @return Boolean, true if a value for every field and WARCInfo object exists, false otherwise
   */
  override def isComplete(): Boolean = {
    if (super.isComplete()) {
      requiredwarcinfo match {
        case Some(x) => return x.isComplete()
        case None => return false
      }
    }
    return false
  }

  /*
   * Returns true if we have a complete WARCInfo object attached, false otherwise
   *
   * @return Boolean, true if attached WARCInfo is present and complete, false otherwise
   */
  def warcInfoComplete(): Boolean = {
    requiredwarcinfo match {
      case Some(x) => return x.isComplete()
      case None => return false
    }
  }

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
