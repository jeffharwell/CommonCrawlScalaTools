package com.jeffharwell.commoncrawl.createcorpus


/*
 * WARCConversion Factory Companion Object
 *
 * This is needed in order to implement multiple constructors and
 * encapsulate the definition of the EmptyCategorizer class.
 *
 * http://daily-scala.blogspot.com/2009/11/multiple-constructors.html
 */
object WARCConversion {
  /*
   * When no arguments are supplied create a class that implements WARCCategorizer
   * and never categorizes anything and pass that to the constructor.
   */
  def apply(): WARCConversion = {
    // Define the absolute minimum WARCCategorizer, it doesn't do anything
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

    new WARCConversion(new EmptyCategorizer)
  } 

  /*
   * In this case an object that is a sub-type of WARCCategorizer has specified
   * been passed, so just pass that to the WARCConversion constructor
   */
  def apply[A <: WARCCategorizer](c: A): WARCConversion = {
    new WARCConversion(c)
  } 
} 


class WARCConversion(acategorizer: WARCCategorizer) extends WARCRecord {

  val categorizer = acategorizer

  // Our implementation of the WARCCategorizer, just wrap the categorizer that we were passed
  // this feels a bit boilerplate'ie but I want to be able to treat the WARCRecord as a 
  // categorizer itself.
  def hasCategories(): Boolean = {
    categorizer.hasCategories()
  }
  def categorize(s: String): WARCCategorizer = {
    categorizer.categorize(s)
  }

  override val requiredfields: List[String] = List[String](
                                              "WARC-Type",
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
   * A WARCRecord should return a set of categories if applicable
   *
   * @return a Option[Set[String]]
   */
  override def getCategories(): Option[Set[String]] = {
    // Wrapping this in makes it consistent with the way that the .get("field")
    // method works. Also makes it explicit when there is actually not categories
    // ,Option(None), vs the record doesn't match any categories, Option(Set())
    Some(categorizer.getCategories())
  }

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
    // how on earth to I refactor this to avoid return??
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
    fields("Content-Length").toInt
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
      if (k == "WARC-Type" && v != "conversion") {
        throw new WARCRecordTypeException(s"Expecting WARC-Type = conversion but found ${v} instead")
      }
    }
  }

  /*
   * Adds the content of the WET Record
   *
   * @param cs the content to be added to the record
   * @return Unit
   */
  override def addContent(cs: String): Unit = {
    fields += "Content" -> cs
    categorize(cs)
  }


}
