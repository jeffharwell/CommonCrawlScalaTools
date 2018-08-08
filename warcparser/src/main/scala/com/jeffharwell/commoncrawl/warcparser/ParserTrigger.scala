package com.jeffharwell.commoncrawl.warcparser

/*
 * The WARC Categorizer Trait
 *
 * Defines an interface (Java speak) that must be implemented by all WARC Categorizers
 */
trait ParserTrigger extends java.io.Serializable {
  /*
   * This method is the one that will be called by the parser so it must
   * be implemented.
   *
   * @param extractedfilename the filename that was extracted from the file itself
   * @param recordcount the number of WARCConversion records the parser has parsed thus far
   * @param logmessage a log message from the parser, usually additional info if the file was corrupt somehow
   */
  def call(extractedfilename: Option[String], recordcount: Integer, logmessage: Option[String]): Unit
}
