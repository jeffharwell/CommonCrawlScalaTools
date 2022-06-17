package com.jeffharwell.commoncrawl.createcorpus.commoncrawlimport

/*
 * This is the ... I think of it as a binding class ... that matches the WARC Record fields with something that can be written to Cassandra.
 * If memory serves this is a kind of template that describes the schema of the table in Cassandra. When I instantiate the class I use the
 * method calls from the WARCRecord object. I can then write this WARCCassandraBind object directly to Cassandra.
 */

case class WETPathsCassandraBind(wet_path:String)