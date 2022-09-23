package com.jeffharwell.commoncrawl.createcorpus.commoncrawlimport

/*
 * Hold miscellaneous bind class for various Cassandra tables.
 */

// Case class representing the pilotparse.classified_warc_document table in Cassandra
case class ClassifiedWARCDocument(warc_record_id:Option[String], category:String, core_keywords:Int, secondary_keywords:Int)

case class WARCRecordIDTable(warc_record_id: String)

case class WARCRecordIDTableOption(warc_record_id: Option[String])

case class WETPathTable(wet_path: String)