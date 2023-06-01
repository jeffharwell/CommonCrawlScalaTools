package com.jeffharwell.commoncrawl.createcorpus.commoncrawlimport

import com.datastax.spark.connector.writer.WriteConf
import com.datastax.spark.connector.{toRDDFunctions, toSparkContextFunctions}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/*
 * Runs a set of WET records in a table through the PrepareDocument class to clean them up.
 * It then writes the cleaned records to a new table.
 */

object prepareDocumentsFromTable {

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
      .set("spark.cassandra.output.concurrent.writes", "3")
      .set("spark.cassandra.output.consistency.level", "ONE")

    val sc = new SparkContext(conf)
    // We need to ignore any nulls, we are never inserting old data so we don't need to overwrite,
    // and we don't want to insert tombstones by inserting null values, as this causes problems with
    // performance that require compaction, which causes problems with disk space.
    // https://github.com/datastax/spark-cassandra-connector/blob/master/doc/5_saving.md
    val ignoreNullsWriteConf = WriteConf.fromSparkConf(sc.getConf).copy(ignoreNulls = true)

    print("\n\n>>>>>> START OF PROGRAM <<<<<<\n\n")
    println("Prepare the documents in pilotparse.wetrecord_refiltered")
    println("Running on Spark version: " + sc.version)

    // Get all the WET Records by category
    val recordsrdd = sc.cassandraTable("pilotparse","wetrecord_refiltered")

    /*
    case class WARCCassandraBindWithCategoriesString(warc_record_id:Option[String], categories:Option[String],
                                               content:Option[String], content_length:Option[String],
                                               content_type:Option[String], lucene:Option[String],
                                               warc_date:Option[String],
                                               wet_path:Option[String], warc_target_uri:Option[String],
                                               warc_tldn:Option[String])
     */

    // There is no elbow for the graph of the document length. For the abortion topic, by the time you
    // have gotten to about 20,000 the counts of documents is falling quickly and by 80,000 or 90,000 you
    // only have a handful of documents in each histogram bucket (30 documents or 50 documents) and while there
    // are a few documents that are extremely long (200,000 characters) their number is quite small and there is
    // probably something wrong with them.
    // On the other hand there is a fairly large number of documents that are very small (200 characters or below)
    // which are also not very helpful as they tend to be navigation or preview fragments of text. So we set the
    // minimum length requirement to 500, and the maximum to 80,000.
    // The file 'size_distribution_of_filtered_abortion_docs' in figures directory of the Google drive has the raw
    // date for the content size histogram used to do this estimate.
    def isCorrectLength(maybeString: Option[String]): Boolean = {
      maybeString match {
        case Some(s) => if (s.length >= 500 & s.length <= 80000) true else false
        case None => false
      }
    }

    def hasContent(maybeString: Option[String]): Boolean = {
      maybeString match {
        case Some(s) => if (s.nonEmpty) true else false
        case None => false
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
    val newrecords: RDD[WARCCassandraBindWithCategoriesString] = recordsrdd.map(x => WARCCassandraBindWithCategoriesString(
      Option(x.get[String]("warc_record_id")),
      Option(x.get[String]("categories")),
      Option(PrepareDocument(x.get[String]("content")).prepare()),
      Option(x.get[String]("content_length")),
      Option(x.get[String]("content_type")), None,
      Option(x.get[String]("warc_date")),
      Option(x.get[String]("wet_path")),
      Option(x.get[String]("warc_target_uri")),
      Option(x.get[String]("warc_tldn"))
    )).filter(x => hasContent(x.content))

    // And create our index structure
    val newrecords_index = newrecords.map(x => WARCIndexCassandraBindWithCategoriesString(x.warc_record_id, x.categories))

    // Now write out our records
    newrecords.saveToCassandra("pilotparse", "wetrecord_refiltered_prepared", writeConf = ignoreNullsWriteConf)
    newrecords_index.saveToCassandra("pilotparse", "wetrecord_index_refiltered_prepared", writeConf = ignoreNullsWriteConf)

    print("\n\n>>>>>> END OF PROGRAM <<<<<<\n\n")
  }
}