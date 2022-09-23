package com.jeffharwell.commoncrawl.createcorpus.commoncrawlimport

import com.datastax.driver.core.{ConsistencyLevel, ResultSet}
import com.datastax.spark.connector.cql.{CassandraConnector, CassandraConnectorConf}
import com.datastax.spark.connector.toSparkContextFunctions
import com.datastax.spark.connector.writer.WriteConf
import com.jeffharwell.commoncrawl.warcparser.{FourForumsWARCStreamFilter, FourForumsWARCTopicFilter, IdentityTopicFilter, StreamFilterStatsWriter, TopicFilterStatsWriter}
import org.apache.spark.{SparkConf, SparkContext}

object evaluateTopicFilterRandomWETPaths {
  def main(args: Array[String]) {

    // First set up the Spark context and point it to the Cassandra cluster.
    // The DNS name cassandra.default.svc.cluster.local resolves to the correct Cassandra
    // cluster within Kubernetes
    val conf = new SparkConf()
      .setAppName("topicfilterevaluation")
      .set("spark.cassandra.connection.host", "cassandra.default.svc.cluster.local")
    val sc = new SparkContext(conf)
    val ignoreNullsWriteConf: WriteConf = WriteConf.fromSparkConf(sc.getConf).copy(ignoreNulls = true,
      consistencyLevel = ConsistencyLevel.ONE)

    val cassandraconf = CassandraConnectorConf(sc.getConf)
    val cc = new CassandraConnector(cassandraconf)


    print("\n\n>>>>>> START OF PROGRAM <<<<<<\n\n");
    println("Evaluating Stream Filter");
    println("Running on Spark version: " + sc.version)

    /*
    CREATE TABLE pilotparse.ten_random_wetpaths (
    wet_path text PRIMARY KEY)
     */
    cc.withSessionDo { session =>
      var result: ResultSet = session.execute("truncate pilotparse.stream_filter_stats_compact")
      println(result.getExecutionInfo)
      result = session.execute("truncate pilotparse.topic_filter_stats")
      println(result.getExecutionInfo)
    }

    // 1) Load up the WET paths from the random_wetpaths table
    //val wet_paths = sc.cassandraTable("pilotparse", "ten_random_wetpaths")
    val wet_paths = sc.cassandraTable("pilotparse", "random_wetpaths")
    val number_of_wet_paths = wet_paths.count()
    print(s"Evaluating against $number_of_wet_paths WET URLs from pilotparse.random_wetpaths table.")

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

    /*
     * Topic Filter Setup
     */

    val topic_filter = new FourForumsWARCTopicFilter()
    println("\n\n>> Configuring Thresholds for topic filter")
    topic_filter.setMentions("abortion", Map("core" -> 1, "secondary" -> 1))
    topic_filter.setMentions("evolution", Map("core" -> 1, "secondary" -> 2))
    topic_filter.setMentions("existenceofgod", Map("core" -> 1, "secondary" -> 1))
    topic_filter.setMentions("guncontrol", Map("core" -> 1, "secondary" -> 1))

    print("\n\n>> Setting up TopicFilterStatsWriter\n\n")
    val topic_stats_writer: TopicFilterStatsWriter = new TopicFilterStatsWriter(cassandraconf)
    topic_filter.setStatsWriter(topic_stats_writer)

    /*
     * Stream Filter Setup
     */

    val stream_filter = new FourForumsWARCStreamFilter()
    println("\n\n>> Configuring Thresholds for stream filter to one keyword mention")
    stream_filter.setMinMentions(1)
    println("\n\n>> Configuring stream filter stats writer")
    val stream_stats_writer: StreamFilterStatsWriter = new StreamFilterStatsWriter(cassandraconf)
    stream_filter.setStatsWriter(stream_stats_writer)

    /*
     * Setup the pipeline and run the parse
     */

    // Set Up our URLs - we want to create an RDD that has the URLs we
    // want to process in partitions so that we can send them out to the
    // worker nodes.
    val url_list: List[String] = wet_paths.map{x => x.get[String]("wet_path")}.collect().toList
    val urls = sc.parallelize(url_list, 10)

    // Set up the pipeline
    val cc_url: String = "https://data.commoncrawl.org"
    val dcc = new ProcessWETPaths(cc_url, sc.getConf)
    val parsed_records_rdd_of_lists = urls.map(x => dcc.parseWETArchiveURL(x, cassandraconf, topic_filter, stream_filter))

    // This actually does the parse (flatMap(identity) turns and RDD[List[WARC...]] into RDD[WARC...]
    val parse_start_time = System.nanoTime()
    val total_records = parsed_records_rdd_of_lists.flatMap(identity).count()
    val parse_end_time = System.nanoTime()

    // Write some summary information
    println(s"Finished Processing $number_of_wet_paths WET Paths containing $total_records WARC conversion records.")
    val elapsed = parse_end_time - parse_start_time
    println(s"Elapsed time for total parse: ${elapsed} ns")

    // Sleep for 60 seconds to allow any async io to wrap up
    Thread.sleep(60000)

    print("\n\n>>>>>> END OF PROGRAM <<<<<<\n\n")
  }
}