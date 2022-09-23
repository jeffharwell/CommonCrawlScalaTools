package com.jeffharwell.commoncrawl.createcorpus.commoncrawlimport

import com.datastax.driver.core.ConsistencyLevel
import com.datastax.spark.connector.toRDDFunctions
import com.datastax.spark.connector.writer.WriteConf
import org.apache.spark.{SparkConf, SparkContext}

/*
 * Small Object that selects a number (currently 50) of random wet paths from the list
 * and puts them into the pilotparse.random_wetpaths table in Cassandra.
 *
 * This is primarily useful if you want to run a process against a number of WET files
 * using different parameters in order to compare results.
 */

object createListOfRandomWETPaths {
  def main(args: Array[String]) {

    // First set up the Spark context and point it to the Cassandra cluster.
    // The DNS name cassandra.default.svc.cluster.local resolves to the correct Cassandra
    // cluster within Kubernetes
    val conf = new SparkConf()
      .setAppName("importcommoncrawl")
      .set("spark.cassandra.connection.host", "cassandra.default.svc.cluster.local")
    val sc = new SparkContext(conf)

    print("\n\n>>>>>> START OF PROGRAM <<<<<<\n\n");
    println("Creating a list of Random WET Paths");
    println("Running on Spark version: " + sc.version)

    // Get a few random wetpaths that we have not processed yet
    val getwetpaths = new GetWETPaths(sc)
    val r = getwetpaths.getRandomWETPaths(100)

    // the x._2 is a Boolean which indicates whether we have finished processing the path yet,
    // this will always be false because of the way getRandomWETPaths is implemented, and we don't care
    // for this application, just drop it and take the string and use that to instantiate the bind class.
    val path_records = r.map(x => WETPathsCassandraBind(x._1))

    // Create the RDD and then write the strings to our Cassandra table.
    val rdd = sc.parallelize(path_records)
    rdd.saveToCassandra("pilotparse", "random_wetpaths")

    print("\n\n>>>>>> END OF PROGRAM <<<<<<\n\n");
  }
}