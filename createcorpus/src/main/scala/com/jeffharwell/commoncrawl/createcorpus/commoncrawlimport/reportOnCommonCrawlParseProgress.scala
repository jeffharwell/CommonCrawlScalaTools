package com.jeffharwell.commoncrawl.createcorpus.commoncrawlimport

import com.datastax.spark.connector.toSparkContextFunctions
import org.apache.spark.{SparkConf, SparkContext}

object reportOnCommonCrawlParseProgress {
  def main(args: Array[String]) {

    // First set up the Spark context and point it to the Cassandra cluster.
    // The DNS name cassandra.default.svc.cluster.local resolves to the correct Cassandra
    // cluster within Kubernetes
    val conf = new SparkConf()
      .setAppName("importcommoncrawlprogress")
      .set("spark.cassandra.connection.host", "cassandra.default.svc.cluster.local")
    val sc = new SparkContext(conf)

    print("\n\n>>>>>> START OF PROGRAM <<<<<<\n\n");
    println("Reporting on Common Crawl Parse Progress");
    println("Running on Spark version: " + sc.version)

    // Get the raw counts from Cassandra. Now we are going to do all the filtering on the spark side
    val pathsrdd = sc.cassandraTable("pilotparse","wetpaths")
    var allpaths = pathsrdd.map{x => (x.get[String]("wet_path"), x.get[Boolean]("finished"), x.get[Boolean]("started"))}
    //var wetpaths = pathsrdd.map{x => x.get[String]("wet_path")}
    var finished = allpaths.filter{x => x._2 == true}
    var errored = allpaths.filter{x => x._2 == false && x._3 == true}
    var started = allpaths.filter{x => x._3 == true}

    // Do some math
    var total_count = allpaths.collect.size
    var finished_count = finished.collect.size
    var remaining_count = total_count - finished_count
    var errored_count = errored.collect.size
    var started_count = started.collect.size
    var percent_sample = (finished_count.toDouble / total_count) * 100
    var percent_unsampled = (remaining_count.toDouble / total_count) * 100
    var percent_errored = (errored_count.toDouble / started_count) * 100
    var percent_sample_string = f"$percent_sample%1.2f"
    var percent_unsampled_string = f"$percent_unsampled%1.2f"
    var percent_errored_string = f"$percent_errored%1.2f"

    // Print the results
    println("")
    println(s"${finished_count} of ${total_count} (${percent_sample_string}%) of the Common Crawl Set has been processed.")
    println(s"${remaining_count} of ${total_count} (${percent_unsampled_string}%) remains unprocessed.")
    println(s"${errored_count} of the documents were started but not completed (${percent_errored_string})% failure rate")

    print("\n\n>>>>>> END OF PROGRAM <<<<<<\n\n");
  }
}