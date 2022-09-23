package com.jeffharwell.commoncrawl.createcorpus.commoncrawlimport

import com.datastax.spark.connector.cql.{CassandraConnector, CassandraConnectorConf}
import com.datastax.spark.connector.toRDDFunctions
import com.jeffharwell.commoncrawl.warcparser.{FourForumsWARCStreamFilter, FourForumsWARCTopicFilter}
import org.apache.spark.{SparkConf, SparkContext}

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.collection.mutable.ListBuffer

/*
This object grabs all the WET file URL that are marked as started but not marked as completed (i.e. the parse
started but failed during error at some point in the process) and retries them.
 */
object parseIncompleteWETFilesFromCommoncrawl {
  def main(args: Array[String]) {

    // First set up the Spark context and point it to the Cassandra cluster.
    // The DNS name cassandra.default.svc.cluster.local resolves to the correct Cassandra
    // cluster within Kubernetes
    val conf = new SparkConf()
      .setAppName("processincompletewetfiles")
      .set("spark.cassandra.connection.host", "cassandra.default.svc.cluster.local")
    val sc = new SparkContext(conf)

    print("\n\n>>>>>> START OF PROGRAM <<<<<<\n\n");
    println("Running on Spark version: " + sc.version)

    val cc_url: String = "https://data.commoncrawl.org"

    // Get a few random wetpaths that we have not processed yet
    val getwetpaths = new GetWETPaths(sc)
    val r = getwetpaths.getIncompleteWETPaths
    println(s"Retrieved ${r.length.toString} paths that were not completely parsed on previous runs.")

    // Set Up our URLs - we want to create an RDD that has the URLs we
    // want to process in partitions so that we can send them out to the
    // worker nodes.
    var url = ListBuffer[String]()
    r.map(x => url += x._1)

    var length = url.size

    var urls = {
      if (length <= 27) {
        sc.parallelize(url)
      } else {
        // We want about 3 WET Archive files per partition
        sc.parallelize(url, length / 3)
      }
    }

    // Instantiate the Stream Filter this pipeline run
    println("\n\n>> Configuring FourForumnsWARCStreamFilter as a stream filter.")
    val stream_filter = new FourForumsWARCStreamFilter()
    stream_filter.setMinMentions(1)

    // Instantiate the Topic Filter for this pipeline run
    println("\n\n>> Configuring FourForumnsWARCTopicFilter as topic filter.")
    val topic_filter: FourForumsWARCTopicFilter = new FourForumsWARCTopicFilter()
    println("\n\n>> Configuring Thresholds for topic filter")
    topic_filter.setMentions("abortion", Map("core" -> 1, "secondary" -> 1))
    topic_filter.setMentions("evolution", Map("core" -> 1, "secondary" -> 2))
    topic_filter.setMentions("existenceofgod", Map("core" -> 1, "secondary" -> 1))
    topic_filter.setMentions("guncontrol", Map("core" -> 1, "secondary" -> 1))


    // Be CAREFUL! The SparkContext (the sc) can never touch the RDD in any way or you will get a very difficult to
    // troubleshoot NPE. This means that if you put the sc in any function that gets passed to the workers, or inside
    // a map that is called on an RDD, then, when the statement finally runs on the worker it will fail with a Null
    // Pointer Exception
    //
    // So, as in this case, utilize the sc outside of the RDD and just pass in the final product, in this case
    // the CassandraConf
    //
    // So the ProcessWETPaths object is going to be serialized and sent out to each worker so that it can
    // process each url in its partition. So ProcessWETPaths must be serializable and can't depend directly on
    // the spark context.

    val cassandraconf = CassandraConnectorConf(sc.getConf)
    val dcc = new ProcessWETPaths(cc_url, sc.getConf)
    val parsed_records = urls.map(x => dcc.parseWETArchiveURL(x, cassandraconf, topic_filter, stream_filter))

    // Now do it, parse the records
    //parsed_records.flatMap(identity).saveToCassandra("pilotparse", "wetrecord", writeConf = ignoreNullsWriteConf)
    val parse_start_time = System.nanoTime()
    parsed_records.flatMap(identity).saveToCassandra("pilotparse", "wetrecord")
    val parse_end_time = System.nanoTime()
    val elapsed = parse_end_time - parse_start_time
    print(s"URLs processed: ${urls.count()}")
    print(s"Elapsed time for processing: ${elapsed} ns")

    val uuid = java.util.UUID.randomUUID.toString
    val formatter = DateTimeFormatter.ofPattern("uuuu/MM/dd HH:mm:ss");
    val timestamp = formatter.format(LocalDateTime.now())
    val number_of_urls = urls.count().toString
    val process_name = "parseIncompleteWETFilesFromCommoncrawl"

    print(s"Writing processing run stats to pilotparse.wetrecord_processing_stats")
    val cc = new CassandraConnector(cassandraconf)
    cc.withSessionDo { session =>
      /* Schema
      CREATE TABLE pilotparse.wetrecord_processing_stats (
          row_uuid text PRIMARY KEY,
          process_name text,
          number_of_urls text,
          processing_time_ns bigint,
          processing_date text
      )
      */
      session.execute(s"insert into pilotparse.wetrecord_processing_stats (row_uuid, process_name, number_of_urls, processing_time_ns, processing_date) values ('${uuid}', '${process_name}', '${number_of_urls}', ${elapsed}, '${timestamp}')")
    }

    print("\n\n>>>>>> END OF PROGRAM <<<<<<\n\n");
  }
}