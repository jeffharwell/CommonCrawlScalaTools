package com.jeffharwell.commoncrawl.createcorpus.commoncrawlimport

import com.datastax.spark.connector.writer.WriteConf
import com.datastax.spark.connector.{toRDDFunctions, toSparkContextFunctions}
import com.jeffharwell.commoncrawl.warcparser.FourForumsWARCTopicFilter
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/*
 * This object appliest a newly configured topic filter to the records that are already in the
 * pilotparse.wetrecord table. After applying the new filter any remaining documents (i.e. records
 * that received a category under the criteria configured in the new TopicFilter) will be written
 * to the table pilotparse.wetrecord_refiltered. This process also creates a new index in
 * pilotparse.wetrecord_index_refiltered.
 */

object refilterWETRecords {

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
      .setAppName("refilterwetrecords abortion")
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
    println("Refiltering the documents in pilotparse.wetrecord")
    println("Running on Spark version: " + sc.version)

    // Get all the WET Records by category
    val recordsrdd = sc.cassandraTable("pilotparse","wetrecord")

    // Set up the new TopicFilter
    val topic_filter = new FourForumsWARCTopicFilter()
    topic_filter.setMentions("abortion", Map("core" -> 1, "secondary" -> 1))
    topic_filter.setMentions("evolution", Map("core" -> 1, "secondary" -> 2))
    topic_filter.setMentions("existenceofgod", Map("core" -> 1, "secondary" -> 1))
    topic_filter.setMentions("guncontrol", Map("core" -> 1, "secondary" -> 1))
    topic_filter.setRequireTokenSeparator(true)

    def get_category_string(tf: FourForumsWARCTopicFilter, content: String): Option[String] = {
      /* Creates a new category string based on the content passed to it. If no categories are
       * found it return None
       */
      val cat_string = {
        val c = tf.categorizeAndCountStringReturnString(content)
        if (c == "{}") {
          None
        } else {
          Option(c)
        }
      }
      cat_string
    }

    /*
    case class WARCCassandraBindWithCategoriesString(warc_record_id:Option[String], categories:Option[String],
                                               content:Option[String], content_length:Option[String],
                                               content_type:Option[String], lucene:Option[String],
                                               warc_date:Option[String],
                                               wet_path:Option[String], warc_target_uri:Option[String],
                                               warc_tldn:Option[String])
     */
    // We are only re-filtering the gun control topic with the code below. If you want to refilter another topic, or
    // refilter all topics, then make a change to the filters method after the below map.
    val filtered_records: RDD[WARCCassandraBindWithCategoriesString] = recordsrdd.map(x => WARCCassandraBindWithCategoriesString(
      Option(x.get[String]("warc_record_id")),
      get_category_string(topic_filter, x.get[String]("content")),
      Option(x.get[String]("content")), Option(x.get[String]("content_length")),
      Option(x.get[String]("content_type")), None,
      Option(x.get[String]("warc_date")),
      Option(x.get[String]("wet_path")), Option(x.get[String]("warc_target_uri")),
      Option(x.get[String]("warc_tldn"))
    )).filter(x => get_list_of_categories_option(x.categories).contains("abortion"))

    // Now we filter out every record where the categories are not defined
    // We are doing our filtering directly on the map, so we don't need this
    //val filtered_records = unfiltered_records.filter(x => x.categories.isDefined)


    // And create our index structure
    val filtered_index = filtered_records.map(x => WARCIndexCassandraBindWithCategoriesString(x.warc_record_id, x.categories))

    // Now write out our records
    filtered_records.saveToCassandra("pilotparse", "wetrecord_refiltered", writeConf = ignoreNullsWriteConf)
    filtered_index.saveToCassandra("pilotparse", "wetrecord_index_refiltered", writeConf = ignoreNullsWriteConf)

    print("\n\n>>>>>> END OF PROGRAM <<<<<<\n\n")
  }
}