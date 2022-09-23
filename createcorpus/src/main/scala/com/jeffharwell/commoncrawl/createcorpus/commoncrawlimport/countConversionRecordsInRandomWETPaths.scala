package com.jeffharwell.commoncrawl.createcorpus.commoncrawlimport

import com.datastax.driver.core.ConsistencyLevel
import com.datastax.spark.connector.cql.CassandraConnectorConf
import com.datastax.spark.connector.types.CassandraOption
import com.datastax.spark.connector.{toRDDFunctions, toSparkContextFunctions}
import com.datastax.spark.connector.writer.WriteConf
import com.jeffharwell.commoncrawl.warcparser.{FourForumsWARCTopicFilter, IdentityStreamFilter, IdentityTopicFilter, WARCRecord}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object countConversionRecordsInRandomWETPaths {
  def main(args: Array[String]) {

    // First set up the Spark context and point it to the Cassandra cluster.
    // The DNS name cassandra.default.svc.cluster.local resolves to the correct Cassandra
    // cluster within Kubernetes
    val conf = new SparkConf()
      .setAppName("countconversionrecords")
      .set("spark.cassandra.connection.host", "cassandra.default.svc.cluster.local")
    val sc = new SparkContext(conf)
    val ignoreNullsWriteConf: WriteConf = WriteConf.fromSparkConf(sc.getConf).copy(ignoreNulls = true,
      consistencyLevel = ConsistencyLevel.ONE)

    print("\n\n>>>>>> START OF PROGRAM <<<<<<\n\n");
    println("Parsing Random WET Files from Common Crawl");
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

    val identity_classifier = new IdentityTopicFilter() // categorize every document as a document
    val identity_stream_filter = new IdentityStreamFilter() // pass every document, don't filter anything out
    val cassandraconf = CassandraConnectorConf(sc.getConf)
    val dcc = new ProcessWETPaths(cc_url, sc.getConf)
    val parsed_records_rdd_of_lists = wet_paths.map(x => dcc.parseWETArchiveURL(x.getString("wet_path"),
                                                                                cassandraconf, identity_classifier,
                                                                                identity_stream_filter))

    // Goes from RDD[List[WARCCassandraBindWithCategories]] to RDD[WARCCassandraBindWithCategories]
    // much more efficient to distribute among the nodes instead of passing around 50 lists of 50,000
    // parsed documents.
    val parsed_records_rdd: RDD[WARCCassandraBindWithCategories] = for {
      record_list <- parsed_records_rdd_of_lists
      record <- record_list
    } yield (record)

    // Now grab all the warc record ids from all the WARCCassandraBindWithCategories case classes
    val all_record_ids = for {
      case_class <- parsed_records_rdd
      warc_id <- case_class.warc_record_id
    } yield (warc_id)

    println(s"Total number of warc record ids: ${all_record_ids.count()}")
    println(s"Total distinct warc record ids: ${all_record_ids.distinct().count()}")

    print("\n\n>>>>>> END OF PROGRAM <<<<<<\n\n")
  }
}