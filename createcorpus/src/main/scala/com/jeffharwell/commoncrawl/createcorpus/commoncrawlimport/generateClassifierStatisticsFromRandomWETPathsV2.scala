package com.jeffharwell.commoncrawl.createcorpus.commoncrawlimport

/*
The difference between v1 and v2 of this object is setting setRequireTokenSeparator(true) on the topic filter. This
makes it so that for the core or secondary keyword to match it must have a space or punctuation mark before
or after it. This significantly impacts the number of matches when you have a short keyword like "gun".
 */

import com.datastax.driver.core.ConsistencyLevel
import com.datastax.spark.connector.cql.CassandraConnectorConf
import com.datastax.spark.connector.types.CassandraOption
import com.datastax.spark.connector.writer.WriteConf
import com.datastax.spark.connector.{toRDDFunctions, toSparkContextFunctions}
import com.jeffharwell.commoncrawl.warcparser.{FourForumsWARCTopicFilter, IdentityStreamFilter, IdentityTopicFilter, WARCRecord}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object generateClassifierStatisticsFromRandomWETPathsV2 {
  def main(args: Array[String]) {

    // Bind function, takes our data and binds it to the Cassandra table so that it can be written later
    def bindToCassandraTable(wc: WARCRecord, category: String, core_keywords_count: Int, secondary_keywords_count: Int): ClassifiedWARCDocument = {
      ClassifiedWARCDocument(CassandraOption.unsetIfNone(wc.get("WARC-Record-ID")),
        category,
        core_keywords_count,
        secondary_keywords_count)
    }

    // First set up the Spark context and point it to the Cassandra cluster.
    // The DNS name cassandra.default.svc.cluster.local resolves to the correct Cassandra
    // cluster within Kubernetes
    val conf = new SparkConf()
      .setAppName("importcommoncrawl")
      .set("spark.cassandra.connection.host", "cassandra.default.svc.cluster.local")
    val sc = new SparkContext(conf)
    val ignoreNullsWriteConf: WriteConf = WriteConf.fromSparkConf(sc.getConf).copy(ignoreNulls = true,
      consistencyLevel = ConsistencyLevel.ONE)

    print("\n\n>>>>>> START OF PROGRAM <<<<<<\n\n");
    println("Parsing WET Files from Common Crawl");
    println("Running on Spark version: " + sc.version)

    val cc_url: String = "https://data.commoncrawl.org"

    // 1) Load up the WET paths from the random_wetpaths table
    val wet_paths = sc.cassandraTable("pilotparse", "random_wetpaths")

    // Set Up our URLs - we want to create an RDD that has the URLs we
    // want to process in partitions so that we can send them out to the
    // worker nodes
    /*
    val urls_rdd: RDD[String] = for {
      x <- wet_paths
    } yield (cc_url + x.getString("wet_path"))
     */

    // 2) Download and Parse all of the wetpaths
    //      Need to modify the ProcessWETPaths so that we can include or exclude documents that are not classified
    //      I want to include non-classified documents so that we have a baseline of the number of documents
    //      that have any matches.
    //      RDD of URL -> RDD of ParsedDocuments

    val identity_classifier = new IdentityTopicFilter() // categorize every document as a document
    val identity_stream_filter = new IdentityStreamFilter() // pass every document, don't filter anything out
    val cassandraconf = CassandraConnectorConf(sc.getConf)
    val dcc = new ProcessWETPaths(cc_url, sc.getConf)
    val parsed_records_rdd_of_lists = wet_paths.map(x => dcc.parseWETArchiveURL(x.getString("wet_path"),
                                                                                cassandraconf, identity_classifier,
                                                                                identity_stream_filter))

    // Goes from RDD[List[WARCCassandraBindWithCategories]] to RDD[WARCCassandraBindWithCategories]
    // much more efficient to distribute among the nodes instead of passing around 50 lists of 50,000
    // parsed documents.
    val parsed_records_rdd: RDD[WARCCassandraBindWithCategories] = for {
      record_list <- parsed_records_rdd_of_lists
      record <- record_list
    } yield (record)

    // 3) Categorize all texts
    //      We want to raw counts from the classifier, so run each text back through the classifier, probably
    //      want to return a set up tuples (class, core_keyword_count, secondary_keyword_count) with unclassified
    //      documents getting ('unclassified', 0, 0)
    //      RDD Parsed Document -> RDD of ClassifiedWARCDocument

    // Create our classifier, if the document contains a core keyword, we classify it. It doesn't matter too much
    // though, because we we are interested in is the count of all string matches for each category. This will be
    // output regardless of whether or not a document is classified.
    val ffc: FourForumsWARCTopicFilter = new FourForumsWARCTopicFilter()
    // Require that the keyword be either proceeded or followed by a space or punctuation mark
    // For all topics and types we are allowing a partial subword match (i.e. the word we are searching
    // for appears at the beginning or end of a word) except for with gun control. For gun control we have
    // two core words we are searching for (gun, and guns) and we require a non-subword match for at least
    // one of them. Otherwise words like 'begun' create lots of false positives.
    val separator_requirements = Map("abortion" -> Map("core" -> 1, "secondary" -> 1),
                                     "evolution" -> Map("core" -> 1, "secondary" -> 1),
                                     "existenceofgod" -> Map("core" -> 1, "secondary" -> 1),
                                     "guncontrol" -> Map("core" -> 2, "secondary" -> 1)
                                    )
    ffc.setRequireTokenSeparator(separator_requirements)

    def categorizeAndBind(warc_record_id: String, document_content: String): List[ClassifiedWARCDocument] = {
      // Even if no documents get categorized the categorizer will spit out a data structure that contains the
      // each category and the number of core and secondary words that matched in the document. This is the structure
      // we want to persist to cassandra. For a given category we can look at all the document and tell which
      // documents did not match at all, and then the counts of core and secondary words for any matches at all.
      val output = ffc.categorizeAndCountString(document_content)
      val l = for {
        x <- output._2 // Map[String, (Int, Int)] containing category name and match counts, Map[Category Name, (count of core matches, count of secondary matches)]
      } yield ClassifiedWARCDocument(Some(warc_record_id), x._1, x._2._1, x._2._2)
      l.toList
    }

    val classifications: RDD[List[ClassifiedWARCDocument]] = for {
      warcbind <- parsed_records_rdd
      content <- warcbind.content
      id <- warcbind.warc_record_id
    } yield categorizeAndBind(id, content)

    // 4) Write to Cassandra
    classifications.flatMap(identity).saveToCassandra("pilotparse", "classified_warc_document_v2")

    print("\n\n>>>>>> END OF PROGRAM <<<<<<\n\n")
  }
}