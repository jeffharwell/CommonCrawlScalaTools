package com.jeffharwell.commoncrawl.createcorpus.commoncrawlimport

import com.datastax.spark.connector.writer.WriteConf
import com.datastax.spark.connector.{toRDDFunctions, toSparkContextFunctions}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/*
 * Runs a set of WET records in a table through the PrepareDocument class to clean them up.
 * It then writes the cleaned records to a new table.
 */

object buildContentLengthTable {

  def main(args: Array[String]): Unit = {
    // First set up the Spark context and point it to the Cassandra cluster.
    // The DNS name cassandra.default.svc.cluster.local resolves to the correct Cassandra
    // cluster within Kubernetes
    // We want a small fetch size because the WARC records are quite large and it will
    // bog down the cluster if you try to fetch a bunch of rows at once.
    val conf = new SparkConf()
      .setAppName("prepare documents")
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
    println("Calculating Content Lengths From in pilotparse.wetrecord_refiltered_prepared")
    println("Running on Spark version: " + sc.version)

    // Get all the WET Records by category
    val recordsrdd = sc.cassandraTable("pilotparse","wetrecord_refiltered_prepared")

    /*
    case class WARCCassandraBindWithCategoriesString(warc_record_id:Option[String], categories:Option[String],
                                               content:Option[String], content_length:Option[String],
                                               content_type:Option[String], lucene:Option[String],
                                               warc_date:Option[String],
                                               wet_path:Option[String], warc_target_uri:Option[String],
                                               warc_tldn:Option[String])
     */


    // Calculates the length of the Option[String] content. If it is None then
    // we return 0.
    def calculateOptionLength(maybeString: Option[String]): Int = {
      maybeString match {
        case Some(s) => s.length
        case None => "0".toInt
      }
    }
    // Run the content from each document through the PrepareDocument class.
    /*
    val newrecords: RDD[WARCCassandraBindWithCategoriesString] = recordsrdd.map(x => WARCCassandraBindWithCategoriesString(
      Option(x.get[String]("warc_record_id")),
      Option(x.get[String]("categories")),
      Option(PrepareDocument(x.get[String]("content")).prepare()),
      Option(x.get[String]("content_length")),
      Option(x.get[String]("content_type")), None,
      Option(x.get[String]("warc_date")),
      Option(x.get[String]("wet_path")), Option(x.get[String]("warc_target_uri")),
      Option(x.get[String]("warc_tldn"))
    )).filter(x => isCorrectLength(x.content))
     */

    // For the first PrepareDocument run we don't have a length filter since we don't know the length
    // distributions of the corpus just yet. However, we do filter out any records that, after PrepareDocument
    // is applied, have no content remaining.
    val records: RDD[WARCCassandraBindWithCategoriesString] = recordsrdd.map(x => WARCCassandraBindWithCategoriesString(
      Option(x.get[String]("warc_record_id")),
      Option(x.get[String]("categories")),
      Option(x.get[String]("content")),
      Option(x.get[String]("content_length")),
      Option(x.get[String]("content_type")), None,
      Option(x.get[String]("warc_date")),
      Option(x.get[String]("wet_path")),
      Option(x.get[String]("warc_target_uri")),
      Option(x.get[String]("warc_tldn"))
    ))

    // And create our index structure
    val length_records = records.map(x => WARCIndexCassandraBindCategoriesLength(x.warc_record_id, x.categories, calculateOptionLength(x.content)))

    // Now write out our records
    length_records.saveToCassandra("pilotparse", "wetrecord_length_refiltered_prepared", writeConf = ignoreNullsWriteConf)

    print("\n\n>>>>>> END OF PROGRAM <<<<<<\n\n")
  }
}