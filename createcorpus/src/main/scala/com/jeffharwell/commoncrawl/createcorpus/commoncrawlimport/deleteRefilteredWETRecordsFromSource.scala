package com.jeffharwell.commoncrawl.createcorpus.commoncrawlimport

import com.datastax.spark.connector.writer.WriteConf
import com.datastax.spark.connector.{toRDDFunctions, toSparkContextFunctions}
// import com.jeffharwell.commoncrawl.warcparser.{FourForumsWARCTopicFilter, IdentityTopicFilter}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/*
 * After we have refiltered the records we can delete all of the records that we refiltered. In this
 * specific case we refiltered all of the 'gun control' records, so we can delete every record from
 * pilotparse.wetrecord that has a category of gun control
 */


object deleteRefilteredWETRecordsFromSource {

  def get_list_of_categories_option(s: Option[String]): List[String] = {
    // If we don't have a string containing our categories, then we
    // just return an empty list. No categories. Otherwise we call the
    // get_list_of_categories function to return our list of categories.
    s match {
      case Some(s) => get_list_of_categories(s)
      case None => List[String]()
    }
  }

  def get_list_of_categories(s: String): List[String] = {
    // Because JSON is not a first class citizen in Scala, and I don't want to add a dependency
    // on some giant framework like Lift, we will just do the parse by hand. It is a simple
    // pattern
    //
    // The category string looks something like: "{'existenceofgod','guncontrol'}" which needs
    // to be converted to a list List('existenceofgod', 'guncontrol')
    s.split("','").map(x => x.replaceAll("[}{']", "")).toList
  }

  def main(args: Array[String]): Unit = {
    // First set up the Spark context and point it to the Cassandra cluster.
    // The DNS name cassandra.default.svc.cluster.local resolves to the correct Cassandra
    // cluster within Kubernetes
    // We want a small fetch size because the WARC records are quite large and it will
    // bog down the cluster if you try to fetch a bunch of rows at once.
    val conf = new SparkConf()
      .setAppName("deleteRefilteredWetRecordsFromSource")
      .set("spark.cassandra.connection.host", "cassandra.default.svc.cluster.local")
      //.set("spark.cassandra.input.fetch.size_in_rows", "20")
      .set("spark.cassandra.input.fetch.size_in_rows", "80")
      .set("spark.cassandra.read.timeout_ms", "60000")
      .set("spark.cassandra.query.retry.count", "120")

    val sc = new SparkContext(conf)
    // We need to ignore any nulls, we are never inserting old data so we don't need to overwrite,
    // and we don't want to insert tombstones by inserting null values, as this causes problems with
    // performance that require compaction, which causes problems with disk space.
    // https://github.com/datastax/spark-cassandra-connector/blob/master/doc/5_saving.md
    val ignoreNullsWriteConf = WriteConf.fromSparkConf(sc.getConf).copy(ignoreNulls = true)

    print("\n\n>>>>>> START OF PROGRAM <<<<<<\n\n")
    println("Deleting refiltered WET records from pilotparse.wetrecord")
    println("Running on Spark version: " + sc.version)

    // The Tuple1 part of this is very important, otherwise obscure errors will occcur
    // https://stackoverflow.com/questions/41031286/multiple-constructors-with-the-same-number-of-parameters-exception-while-transfo
    print("\nGetting Records to Delete\n\n")

    // Get all the WET Records by category
    val recordsrdd = sc.cassandraTable("pilotparse", "wetrecord")

    // Only grab gun control records
    val gun_control_records: RDD[WARCCassandraBindWithCategoriesString] = recordsrdd.map(x => WARCCassandraBindWithCategoriesString(
      Option(x.get[String]("warc_record_id")),
      Option(x.get[String]("categories")),
      Option(x.get[String]("content")), Option(x.get[String]("content_length")),
      Option(x.get[String]("content_type")), None,
      Option(x.get[String]("warc_date")),
      Option(x.get[String]("wet_path")), Option(x.get[String]("warc_target_uri")),
      Option(x.get[String]("warc_tldn"))
    )).filter(x => get_list_of_categories_option(x.categories).contains("guncontrol"))
    val to_delete = gun_control_records.map(x => Tuple1(x.warc_record_id))

    print("\nDeleting Records\n\n")
    // Delete the gun control records from the table
    to_delete.deleteFromCassandra("pilotparse", "wetrecord")

    print("\n\n>>>>>> END OF PROGRAM <<<<<<\n\n")
  }
}