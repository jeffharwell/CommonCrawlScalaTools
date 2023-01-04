package com.jeffharwell.commoncrawl.createcorpus.commoncrawlimport

import com.datastax.spark.connector.toSparkContextFunctions
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object reportOnDocumentsParsedByCategoryUsingIndex {
  def main(args: Array[String]): Unit = {

    // First set up the Spark context and point it to the Cassandra cluster.
    // The DNS name cassandra.default.svc.cluster.local resolves to the correct Cassandra
    // cluster within Kubernetes
    val conf = new SparkConf()
      .setAppName("reportdocumentsbycategoryusingindex")
      .set("spark.cassandra.connection.host", "cassandra.default.svc.cluster.local")
    val sc = new SparkContext(conf)

    print("\n\n>>>>>> START OF PROGRAM <<<<<<\n\n")
    println("Reporting on the Number of Common Crawl Documents Parsed by Category")
    println("Running on Spark version: " + sc.version)

    // Get all the WET Records by category
    val recordsrdd = sc.cassandraTable("pilotparse","wetrecord_index")

    //
    // Don't do it this way, it makes us read through every record in Cassandra twice,
    // which takes forever.
    //
    //val all_record_ids = recordsrdd.map(x => x.get[String]("warc_record_id"))
    //val count_of_all_records = all_record_ids.count()

    def get_list_of_categories(s: String): List[String] = {
      // Because JSON is not a first class citizen in Scala, and I don't want to add a dependency
      // on some giant framework like Lift, we will just do the parse by hand. It is a simple
      // pattern
      //
      // The category string looks something like: "{'existenceofgod','guncontrol'}" which needs
      // to be converted to a list List('existenceofgod', 'guncontrol')
      s.split("','").map(x => x.replaceAll("[}{']", "")).toList
    }

    val records_by_category = recordsrdd.map(x => (x.get[String]("warc_record_id"), get_list_of_categories(x.get[String]("categories"))))
    // It seems like this is recomputed for every count, lets see if caching the
    // data structure from Cassandra speeds things up.
    // This will save the data as a serialized object, saving memory at the cost of a bit
    // of CPU. If the cluster runs out of RAM it will write remaining partitions
    // to a disk cache as serialized objects.
    records_by_category.persist(StorageLevel.MEMORY_AND_DISK_SER)

    // First count the number of records that we have (this should also cache the data structure)
    // So this operation will take a while, but the category counts should be much faster.
    val count_of_all_records = records_by_category.count()

    def one_if_contains(s: (String, List[String]), p: String): Int = { if (s._2.contains(p)) { 1 } else { 0 }}

    val categories = List("guncontrol", "abortion", "evolution", "existenceofgod")

    def count_documents(category: String): Int = {
      records_by_category.map{x => one_if_contains(x, category)}.reduce(_ + _)
    }

    // Now count the number of documents per category. The count_documents closure uses
    // the cached records_by_category RDD
    val category_and_count = categories.map(c => c -> count_documents(c)).toMap

    println("-----------------------------")
    println(s"${count_of_all_records} distinct documents have been collected.")
    println("Count of documents per category (some documents may be in more than one category.)")
    println("Category: Number of Documents")
    for ((category, count) <- category_and_count) {
      println(s"$category: $count")
    }

    // Remove the RDD from memory and free any disk space used
    records_by_category.unpersist()

    print("\n\n>>>>>> END OF PROGRAM <<<<<<\n\n")
  }
}