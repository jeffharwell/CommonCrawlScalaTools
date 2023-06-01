package com.jeffharwell.commoncrawl.createcorpus.commoncrawlimport

import com.datastax.spark.connector.writer.WriteConf
import com.datastax.spark.connector.{toRDDFunctions, toSparkContextFunctions}
import com.jeffharwell.commoncrawl.warcparser.FourForumsWARCTopicFilter
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable

import java.util.regex.{MatchResult, Matcher, Pattern}

/*
 * The idea is to get a list of all the subwords that the filter finds. As of 2023.04.02 this object
 * is still a work in progress.
 */

object getSubwordsFromWETRecords {

  case class SubwordOccurances(subword: String, count: Int)

  val punctuation_pattern = Pattern.compile("(^\\p{Punct}*)(.*)($\\p{Punct}*)")
  def remove_punctuation(token: String): String = {
    val my_match: Matcher = punctuation_pattern.matcher(token)
    val my_result: MatchResult = my_match.toMatchResult
    my_result.group(1).toLowerCase()
  }

  def count_subwords_option(subword: String, text: Option[String]): Map[String, Int] = {
    val x = for {
      t <- text
    } yield(count_subwords(subword, t))
    // If we found no subwords, especially if we don't have a text to begin with,
    // then we return an empty map
    x match {
      case Some(m) => m
      case None => Map[String, Int]()
    }
  }

  def count_subwords(subword: String, text: String): Map[String, Int] = {
    val lc_subword = subword.toLowerCase()
    val subword_begins = Pattern.compile(s"^$lc_subword.*")
    val subword_ends = Pattern.compile(s".*$lc_subword$$")
    val tokenized: Array[String] = text.split(" ")
    val token_no_punct: Array[String] = tokenized.map{x => remove_punctuation(x)}
    val no_punct: Array[String] = for {
      t: String <- token_no_punct
      if (subword_begins.matcher(t).matches() || subword_ends.matcher(t).matches())
    } yield t
    val mm: scala.collection.mutable.Map[String, Int] = scala.collection.mutable.Map[String, Int]()
    no_punct.foreach{ t =>
      if (mm.contains(t)) {
        mm(t) += 1
      } else {
        mm += ((t, 1))
      }
    }
    mm.toMap
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
    // Get the word we are creating the subwordlist for
    if (args.length < 2) {
      print("\n\n>>>>>>>>> ERROR: Must pass the subword to search for, and the category to search as an argument to spark-submit\n\n")
      print(">>>>>>>> EXITING <<<<<<<<<\n\n")
      throw new RuntimeException("no arguments passed to getSubwordsFromWETRecords")
    }
    val subword: String = args(0)
    val category: String = args(1)

    // First set up the Spark context and point it to the Cassandra cluster.
    // The DNS name cassandra.default.svc.cluster.local resolves to the correct Cassandra
    // cluster within Kubernetes
    // We want a small fetch size because the WARC records are quite large and it will
    // bog down the cluster if you try to fetch a bunch of rows at once.
    val conf = new SparkConf()
      .setAppName("getsubwordlist")
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
    println(s"Creating subword list for '$subword' from the documents in pilotparse.wetrecord in category ${category}")
    println("Running on Spark version: " + sc.version)

    // Get all the WET Records by category
    val recordsrdd = sc.cassandraTable("pilotparse","wetrecord")

    // Set up the new TopicFilter
    val topic_filter = new FourForumsWARCTopicFilter()
    topic_filter.setMentions("abortion", Map("core" -> 1, "secondary" -> 1))
    topic_filter.setMentions("evolution", Map("core" -> 1, "secondary" -> 2))
    topic_filter.setMentions("existenceofgod", Map("core" -> 1, "secondary" -> 1))
    topic_filter.setMentions("guncontrol", Map("core" -> 1, "secondary" -> 1))

    // Since we are on the hunt for subwords, we don't want to filter out any sub-words now do we.
    val separator_requirements = Map("abortion" -> Map("core" -> 0, "secondary" -> 0),
                                     "evolution" -> Map("core" -> 0, "secondary" -> 0),
                                     "existenceofgod" -> Map("core" -> 0, "secondary" -> 0),
                                     "guncontrol" -> Map("core" -> 0, "secondary" -> 0)
                                    )
    topic_filter.setRequireTokenSeparator(separator_requirements)

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
    val unfiltered_records: RDD[WARCCassandraBindWithCategoriesString] = recordsrdd.map(x => WARCCassandraBindWithCategoriesString(
      Option(x.get[String]("warc_record_id")),
      get_category_string(topic_filter, x.get[String]("content")),
      Option(x.get[String]("content")), Option(x.get[String]("content_length")),
      Option(x.get[String]("content_type")), None,
      Option(x.get[String]("warc_date")),
      Option(x.get[String]("wet_path")), Option(x.get[String]("warc_target_uri")),
      Option(x.get[String]("warc_tldn"))
    ))

    def my_category_filter(categories_string: Option[String]): Boolean = {
      val cat_list = categories_string match {
        case Some(cs) => get_list_of_categories(cs)
        case None => List[String]()
      }
      // Closure, picks up the category from the context
      if (cat_list.contains(category)) {
        true
      } else {
        false
      }
    }

    // Now we filter out every record where the categories are not defined
    val filtered_records = unfiltered_records.filter(x => my_category_filter(x.categories))
    // For every document, count the subwords
    val counts_of_subwords: RDD[Map[String, Int]] = filtered_records.map(x => count_subwords_option(subword, x.content))
    // Flatten our Map[String, Int] to a list org.apache.spark.rdd.RDD[(String, Int)]
    val pair_rdd: org.apache.spark.rdd.RDD[(String, Int)] = counts_of_subwords.flatMap(f => f)
    // Aggregate by key and sum up the counts
    val reduced_rdd: org.apache.spark.rdd.RDD[(String, Int)] = pair_rdd.reduceByKey(_ + _)
    // Populate the bind classes for Cassandra
    val records = reduced_rdd.map(x => SubwordOccurances(x._1, x._2))
    // Write the subword counts to Cassandra
    records.saveToCassandra("pilotparse", "gun_subword_count", writeConf = ignoreNullsWriteConf)

    print("\n\n>>>>>> END OF PROGRAM <<<<<<\n\n")
  }
}