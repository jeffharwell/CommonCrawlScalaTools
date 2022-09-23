package com.jeffharwell.commoncrawl.createcorpus.commoncrawlimport

import com.datastax.driver.core.ResultSet
import com.datastax.spark.connector.cql.{CassandraConnector, CassandraConnectorConf}
import com.datastax.spark.connector.{toRDDFunctions, toSparkContextFunctions}
import com.jeffharwell.commoncrawl.warcparser.{IdentityStreamFilter, IdentityTopicFilter}
import org.apache.spark.{SparkConf, SparkContext}

/*
 * Small Object that selects all the URLs from the random wet paths table in Cassandra, parses them
 * to extract the WARC Conversion IDs, and puts them into the pilotparse.random_wetpaths_record_ids
 * table in Cassandra.
 */

object populateRandomWETPathsWARCIDs {
  def main(args: Array[String]) {

    // First set up the Spark context and point it to the Cassandra cluster.
    // The DNS name cassandra.default.svc.cluster.local resolves to the correct Cassandra
    // cluster within Kubernetes
    val conf = new SparkConf()
      .setAppName("populaterandomwetpathwarcids")
      .set("spark.cassandra.connection.host", "cassandra.default.svc.cluster.local")
    val sc = new SparkContext(conf)
    val cassandraconf = CassandraConnectorConf(sc.getConf)
    val cc = new CassandraConnector(cassandraconf)

    print("\n\n>>>>>> START OF PROGRAM <<<<<<\n\n");
    println("Populating a WARC IDs for all URLs in pilotparse.random_wetpaths");
    println("Running on Spark version: " + sc.version)

    /*
     * CREATE TABLE pilotparse.random_wetpaths (wet_path text PRIMARY KEY)
     *
     * CREATE TABLE pilotparse.random_wetpaths_record_ids (warc_record_id text PRIMARY KEY )
     */

    // Set Up our URLs - we want to create an RDD that has the URLs we
    // want to process in partitions so that we can send them out to the
    // worker nodes.
    val wet_paths = sc.cassandraTable("pilotparse", "random_wetpaths")
    val url_list: List[String] = wet_paths.map{x => x.get[String]("wet_path")}.collect().toList
    val urls = sc.parallelize(url_list, 20)  // 20 slices means 5 URLs per slice

    // Now parse all URLs and write the valid WARC Record IDs to the pilotparse.ten_random_wetpaths_record_ids
    val identity_classifier = new IdentityTopicFilter() // categorize every document as a document
    val identity_stream_filter = new IdentityStreamFilter() // pass every document, don't filter anything out
    val cc_url: String = "https://data.commoncrawl.org"
    val dcc = new ProcessWETPaths(cc_url, sc.getConf)

    /*
     * Set up the pipeline
     * First parse the archive url
     * Then use flatmap to go from RDD[List[WARCCassandraBindWithCategories]] to RDD[WARCCassandraBindWithCategories]
     * The use the map to grab the Option(String) that is the warc_record_id and wrap it in a WARCRecordIDTableOption
     * bind class so that we can write it later.
     */
    val parsed_records = urls.map(x => dcc.parseWETArchiveURL(x, cassandraconf, identity_classifier, identity_stream_filter))
    val warc_record_ids = parsed_records.flatMap(identity).map{x => WARCRecordIDTableOption(x.warc_record_id)}

    // Now do it, parse the records, do mapping, and write them into "random_wetpaths_record_ids"
    val parse_start_time = System.nanoTime()
    warc_record_ids.saveToCassandra("pilotparse", "random_wetpaths_record_ids")
    val parse_end_time = System.nanoTime()
    val elapsed = parse_end_time - parse_start_time
    println(s"URLs processed: ${urls.count()}")
    println(s"Elapsed time for processing: ${elapsed} ns")

    print("\n\n>>>>>> END OF PROGRAM <<<<<<\n\n");
  }
}