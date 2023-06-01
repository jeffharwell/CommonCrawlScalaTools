package com.jeffharwell.commoncrawl.createcorpus.commoncrawlimport

/*
 * This is the ... I think of it as a binding class ... that matches the WARC Record fields with something that can be written to Cassandra.
 * If memory serves this is a kind of template that describes the schema of the table in Cassandra. When I instantiate the class I use the
 * method calls from the WARCRecord object. I can then write this WARCCassandraBind object directly to Cassandra.
 */

case class WARCCassandraBindWithCategories(warc_record_id:Option[String], categories:Set[String],
                                           content:Option[String], content_length:Option[String],
                                           content_type:Option[String], lucene:Option[String],
                                           warc_date:Option[String],
                                           wet_path:Option[String], warc_target_uri:Option[String],
                                           warc_tldn:Option[String])
case class WARCCassandraBindWithCategoriesString(warc_record_id:Option[String], categories:Option[String],
                                           content:Option[String], content_length:Option[String],
                                           content_type:Option[String], lucene:Option[String],
                                           warc_date:Option[String],
                                           wet_path:Option[String], warc_target_uri:Option[String],
                                           warc_tldn:Option[String])

case class WARCIndexCassandraBindWithCategoriesString(warc_record_id:Option[String], categories:Option[String])

case class WARCIndexCassandraBindCategoriesLength(warc_record_id:Option[String], categories:Option[String], content_length: Int)

/* I think this is legacy, use the above xxWithCategories bind class */
case class WARCCassandraBind(warc_record_id:Option[String], content:Option[String], content_length:Option[String],
                             content_type:Option[String], lucene:Option[String], warc_date:Option[String], warc_target_uri:Option[String], warc_tldn:String)