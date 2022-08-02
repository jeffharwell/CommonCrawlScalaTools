package com.jeffharwell.commoncrawl.createcorpus.commoncrawlimport

import com.datastax.spark.connector.toSparkContextFunctions
import org.apache.spark.{SparkConf, SparkContext}

object reportOnDocumentsParsedByCategory {
  def main(args: Array[String]): Unit = {

    // First set up the Spark context and point it to the Cassandra cluster.
    // The DNS name cassandra.default.svc.cluster.local resolves to the correct Cassandra
    // cluster within Kubernetes
    val conf = new SparkConf()
      .setAppName("reportdocumentsbycategory")
      .set("spark.cassandra.connection.host", "cassandra.default.svc.cluster.local")
    val sc = new SparkContext(conf)

    print("\n\n>>>>>> START OF PROGRAM <<<<<<\n\n")
    println("Reporting on the Number of Common Crawl Documents Parsed by Category")
    println("Running on Spark version: " + sc.version)

    // Get all the WET Records by category
    val recordsrdd = sc.cassandraTable("pilotparse","wetrecord")

    val all_record_ids = recordsrdd.map(x => x.get[String]("warc_record_id"))
    val count_of_all_records = all_record_ids.count()

    val records_by_category = recordsrdd.map(x => (x.get[String]("warc_record_id"), x.get[Set[String]]("categories")))
    // It seems like this is recomputed for every count, lets see if caching the
    // data structure from Cassandra speeds things up.
    records_by_category.cache()

    def one_if_contains(s: (String, Set[String]), p: String): Int = { if (s._2.contains(p)) { 1 } else { 0 }}

    val categories = List("guncontrol", "abortion", "evolution", "existenceofgod")

    def count_documents(category: String): Int = {
      records_by_category.map{x => one_if_contains(x, category)}.reduce(_ + _)
    }

    val category_and_count = categories.map(c => c -> count_documents(c)).toMap

    println("-----------------------------")
    println(s"${count_of_all_records} distinct documents have been collected.")
    println("Count of documents per category (some documents may be in more than one category.)")
    println("Category: Number of Documents")
    for ((category, count) <- category_and_count) {
      println(s"$category: $count")
    }

    print("\n\n>>>>>> END OF PROGRAM <<<<<<\n\n")
  }
}