package com.jeffharwell.commoncrawl.createcorpus.commoncrawlimport

import com.datastax.driver.core.ResultSet
import com.datastax.spark.connector.cql.{CassandraConnector, CassandraConnectorConf}
import com.datastax.spark.connector.{toRDDFunctions, toSparkContextFunctions}
import com.jeffharwell.commoncrawl.warcparser.{IdentityStreamFilter, IdentityTopicFilter}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

/*
 * Small Object that selects a number (currently 50) of random wet paths from the list
 * and puts them into the pilotparse.random_wetpaths table in Cassandra.
 *
 * This is primarily useful if you want to run a process against a number of WET files
 * using different parameters in order to compare results.
 */

object populateTenRandomWETPaths {
  def main(args: Array[String]) {

    // First set up the Spark context and point it to the Cassandra cluster.
    // The DNS name cassandra.default.svc.cluster.local resolves to the correct Cassandra
    // cluster within Kubernetes
    val conf = new SparkConf()
      .setAppName("populatetenrandomwetpaths")
      .set("spark.cassandra.connection.host", "cassandra.default.svc.cluster.local")
    val sc = new SparkContext(conf)
    val cassandraconf = CassandraConnectorConf(sc.getConf)
    val cc = new CassandraConnector(cassandraconf)

    print("\n\n>>>>>> START OF PROGRAM <<<<<<\n\n");
    println("Populating a list of 10 Random WET Paths");
    println("Running on Spark version: " + sc.version)

    cc.withSessionDo { session =>
      var result: ResultSet = session.execute("truncate pilotparse.ten_random_wetpaths")
      println(result.getExecutionInfo.getWarnings)
      result = session.execute("truncate pilotparse.ten_random_wetpaths_record_ids")
      println(result.getExecutionInfo.getWarnings)
    }

    // Get a few random wetpaths that we have not processed yet
    val getwetpaths = new GetWETPaths(sc)

    // Select the "first" ten wet paths from the random_wetpaths table
    val r = getwetpaths.selectTenRandomWETPaths().map(x => WETPathTable(x))

    /*
     * CREATE TABLE pilotparse.ten_random_wetpaths (wet_path text PRIMARY KEY)
     *
     * CREATE TABLE pilotparse.ten_random_wetpaths_record_ids (warc_record_id text PRIMARY KEY)
     */

    // Create the RDD and then write the strings to our Cassandra table
    val rdd = sc.parallelize(r)
    rdd.saveToCassandra("pilotparse", "ten_random_wetpaths")

    // Set Up our URLs - we want to create an RDD that has the URLs we
    // want to process in partitions so that we can send them out to the
    // worker nodes.
    val wet_paths = sc.cassandraTable("pilotparse", "ten_random_wetpaths")
    val url_list: List[String] = wet_paths.map{x => x.get[String]("wet_path")}.collect().toList
    val urls = sc.parallelize(url_list, 3)

    // Now parse all 10, and write the valid WARC Record IDs to the pilotparse.ten_random_wetpaths_record_ids
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

    // Now do it, parse the records, do mapping, and write them into "ten_random_wetpaths_record_ids"
    val parse_start_time = System.nanoTime()
    warc_record_ids.saveToCassandra("pilotparse", "ten_random_wetpaths_record_ids")
    val parse_end_time = System.nanoTime()
    val elapsed = parse_end_time - parse_start_time
    println(s"URLs processed: ${urls.count()}")
    println(s"Elapsed time for processing: ${elapsed} ns")

    print("\n\n>>>>>> END OF PROGRAM <<<<<<\n\n");
  }
}