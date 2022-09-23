package com.jeffharwell.commoncrawl.createcorpus.commoncrawlimport

import com.datastax.driver.core.ConsistencyLevel
import com.datastax.spark.connector.cql.{CassandraConnector, CassandraConnectorConf}
import com.datastax.spark.connector.types.CassandraOption
import com.datastax.spark.connector.{toRDDFunctions, toSparkContextFunctions}
import com.datastax.spark.connector.writer.WriteConf
import com.jeffharwell.commoncrawl.warcparser.{FourForumsWARCStreamFilter, FourForumsWARCTopicFilter, IdentityStreamFilter, IdentityTopicFilter, StreamFilterStatsWriter, WARCRecord}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object evaluateStreamFilterRandomWETPaths {
  def main(args: Array[String]) {

    // First set up the Spark context and point it to the Cassandra cluster.
    // The DNS name cassandra.default.svc.cluster.local resolves to the correct Cassandra
    // cluster within Kubernetes
    val conf = new SparkConf()
      .setAppName("streamfilterevaluation")
      .set("spark.cassandra.connection.host", "cassandra.default.svc.cluster.local")
    val sc = new SparkContext(conf)
    val ignoreNullsWriteConf: WriteConf = WriteConf.fromSparkConf(sc.getConf).copy(ignoreNulls = true,
      consistencyLevel = ConsistencyLevel.ONE)

    print("\n\n>>>>>> START OF PROGRAM <<<<<<\n\n");
    println("Evaluating Stream Filter");
    println("Running on Spark version: " + sc.version)

    val cc_url: String = "https://data.commoncrawl.org"

    // 1) Load up the WET paths from the random_wetpaths table
    val wet_paths = sc.cassandraTable("pilotparse", "random_wetpaths")

    // Set Up our URLs - we want to create an RDD that has the URLs we
    // want to process in partitions so that we can send them out to the
    // worker nodes
    /*
    val urls_rdd: RDD[String] = for {
      x <- wet_paths
    } yield (cc_url + x.getString("wet_path"))
     */

    // 2) Download and Parse all of the wetpaths
    //      Need to modify the ProcessWETPaths so that we can include or exclude documents that are not classified
    //      I want to include non-classified documents so that we have a baseline of the number of documents
    //      that have any matches.
    //      RDD of URL -> RDD of ParsedDocuments

    val cassandraconf = CassandraConnectorConf(sc.getConf)
    val dcc = new ProcessWETPaths(cc_url, sc.getConf)

    val identity_classifier = new IdentityTopicFilter() // categorize every document as a document
    val stream_filter = new FourForumsWARCStreamFilter()
    stream_filter.setMinMentions(1)
    val stats_writer: StreamFilterStatsWriter = new StreamFilterStatsWriter(cassandraconf)
    stream_filter.setStatsWriter(stats_writer)

    // Set up the pipeline
    val parsed_records_rdd_of_lists = wet_paths.map(x => dcc.parseWETArchiveURL(x.getString("wet_path"), cassandraconf, identity_classifier, stream_filter))

    // This actually does the parse
    val total_records = parsed_records_rdd_of_lists.count()
    print(s"Finished Processing All WET Paths: Processed ${total_records} WET files.")

    print("\n\n>>>>>> END OF PROGRAM <<<<<<\n\n")
  }
}