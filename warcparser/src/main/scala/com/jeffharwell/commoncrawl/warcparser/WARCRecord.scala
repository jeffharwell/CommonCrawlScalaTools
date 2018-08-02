package com.jeffharwell.commoncrawl.warcparser

import scala.collection.mutable.ListBuffer

// Nothing fancy, just the requirements for implementing the different types of WARC records
// Basically an abstract class

abstract class WARCRecord extends java.io.Serializable {
  // Object Variables ... 
  val requiredfields  = List[String]() // this must we overridden by methods inheriting this trait

  // Holds the fields and their values
  val fields = scala.collection.mutable.Map[String,String]()
  // Data structure to hold any additional fields that the caller might
  // want to add to the object after initialization.
  val additionalrequiredfields: ListBuffer[String] = ListBuffer[String]()

  /*
   * Add optional fields in case you want to add additional fields to 
   * this WARC record in addition to the required ones.
   *
   * @param m A map with the field and value to add
   */
  def addFields(m: scala.collection.immutable.Map[String, String]): Unit

  /*
   * A WARCRecord should return a set of categories if applicable
   *
   * The default implementation returns None
   *
   * @return a Option[Set[String]]
   */
  def getCategories(): Option[Set[String]] = {
    None
  }


  /*
   * Returns the number of fields that are required. This will include Content if
   * that is defined. Don't use this to try to determine if the WARCRecord is complete,
   * for that purpose use isComplete() and headersComplete() instead
   *
   * @return integer representing the number of required fields
   */
  def numberRequiredFields(): Int = {
    return requiredfields.size + additionalrequiredfields.size
  }

  /*
   * Returns true if this object contains this field (and value), false otherwise
   *
   * @param k the field name to check
   * @return Boolean
   */
  def containsField(k: String): Boolean = {
    if (fields.contains(k)) {
      return true
    }
    return false
  }

  /*
   * Returns true if there is a value for each required field and each
   * additional required field added after initialization. False otherwise.
   *
   * @return Boolean, true if a value for every field, false otherwise
   */
  def isComplete(): Boolean = {
    requiredfields.foreach( field =>
        if (!fields.contains(field)) {
          return false
        }
    )
    additionalrequiredfields.foreach( field =>
        if (!fields.contains(field)) {
          return false
        }
    )
    return true
  }

  /*
   * Returns true if there is a value for each header field and each
   * additional required field added after initialization. False otherwise.
   * This boils down to checking everything but the content field.
   *
   * @return Boolean, true if a value for every field, false otherwise
   */
  def headersComplete(): Boolean = {
    requiredfields.foreach( field =>
        if (!fields.contains(field) && field != "Content") {
          return false
        }
    )
    additionalrequiredfields.foreach( field =>
        if (!fields.contains(field)) {
          return false
        }
    )
    return true
  }

  /*
   * Adds a required field to the WET record. isComplete() will not return
   * true until there is a key in the fields map that matches each field in 
   * the requiredfields List as well as in the additionalrequiredfields ListBuffer.
   *
   * @param s String with the name of the field that should be required
   */
  def addRequirement(s: String): Unit = {
    additionalrequiredfields += s
  }


  /*
   * Adds the content of the WET Record
   *
   * @param cs the content to be added to the record
   * @return Unit
   */
  def addContent(cs: String): Unit = {
    fields += "Content" -> cs
  }

  /*
   * Returns the content of the WARC Record
   *
   * @return an Option[String] that has the content if there is any
   */
  def getContent(): Option[String] = {
    if (fields.contains("Content")) {
      Some(fields("Content"))
    } else {
      None
    }
  }

  /*
   * Returns the value of an arbitrary field
   *
   * @return an Option[String] that has the content if there is any
   */
  def get(fieldname: String): Option[String] = {
    if (fields.contains(fieldname)) {
      Some(fields(fieldname))
    } else {
      None
    }
  }

}

