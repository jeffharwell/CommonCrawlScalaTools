package com.jeffharwell.commoncrawl.createcorpus.commoncrawlimport

import com.datastax.spark.connector.{toRDDFunctions, toSparkContextFunctions}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

/*
## Overview

This code assumes that the topical corpus, cleaned by the cleaning SVM, are available in Cassandra. Now that
corpora have been cleaned we need to recalculate the relevance scores. This code will take the corpora in
Cassandra, re-calculate the relevance scores using Cassandra, and then write the output to a MariaDB database
via the JDBC driver.

We will be using the built in Spark SQL functionality to directly persist the dataframe to the database.
 */


object createCorpusPostSVMToMySQL extends java.io.Serializable {

  def main(args: Array[String]): Unit = {

    // Check our arguments
    if (args.length != 3) {
      print("\n\n>>>>>>>>> ERROR: Must pass jdbc string, user, and password for the database\n\n")
      print(">>>>>>>> EXITING <<<<<<<<<\n\n")
      throw new RuntimeException("no arguments pass to parseWETFilesFromCommonCrawl")
    }

    // First set up the Spark context and point it to the Cassandra cluster.
    // The DNS name cassandra.default.svc.cluster.local resolves to the correct Cassandra
    // cluster within Kubernetes
    // We want a small fetch size because the WARC records are quite large and it will
    // bog down the cluster if you try to fetch a bunch of rows at once.
    val conf = new SparkConf()
      .setAppName("prepare documents postSVM")
      .set("spark.cassandra.connection.host", "cassandra.default.svc.cluster.local")
      //.set("spark.cassandra.input.fetch.size_in_rows", "20")
      .set("spark.cassandra.input.fetch.size_in_rows", "80")
      .set("spark.cassandra.read.timeout_ms", "60000")
      .set("spark.cassandra.query.retry.count", "120")
      .set("spark.cassandra.output.concurrent.writes", "3")
      .set("spark.cassandra.output.consistency.level", "ONE")
      .set("spark.sql.broadcastTimeout",  "900") // we are broadcasting some large variables https://stackoverflow.com/questions/41123846/why-does-join-fail-with-java-util-concurrent-timeoutexception-futures-timed-ou

    val sc = new SparkContext(conf)
    val spark = SparkSession.builder.config(sc.getConf).getOrCreate()
    import spark.implicits._

    print("\n\n>>>>>> START OF PROGRAM <<<<<<\n\n")
    println("Copying PostSVM Corpus to MariaDB with Relevance")
    println("Running on Spark version: " + sc.version)

    // Connect to MySQL Database
    // Use sql_mode=ANSI_QUOTES otherwise Mariadb will throw an error when the JDBC attempts
    // to reference columns using double quotes versus backticks
    //val dbUrl = "jdbc:mariadb://XXXX:3306/XXXX?sessionVariables=sql_mode=ANSI_QUOTES"
    //val dbUser = "XXXX"
    //val dbPassword = "XXXXXX"

    val dbUrl: String = args(0)
    val dbUser = args(1)
    val dbPassword = args(2)
    val dbProperties = new java.util.Properties()
    dbProperties.setProperty("user", dbUser)
    dbProperties.setProperty("password", dbPassword)
    dbProperties.setProperty("driver", "org.mariadb.jdbc.Driver")

    // We need to ignore any nulls, we are never inserting old data so we don't need to overwrite,
    // and we don't want to insert tombstones by inserting null values, as this causes problems with
    // performance that require compaction, which causes problems with disk space.
    // https://github.com/datastax/spark-cassandra-connector/blob/master/doc/5_saving.md
    // val ignoreNullsWriteConf = WriteConf.fromSparkConf(sc.getConf).copy(ignoreNulls = true)

    val source_tablespace = "cc_exports"  // tablespace in which to find the source tables
    val dest_tablespace = "cc_exports"  // tablespace in which to find the target tables
    val source_tables = List("abortion_postsvm_20230512",
                             "guncontrol_postsvm_20230512",
                             "evolution_postsvm_20230512",
                             "existenceofgod_postsvm_20230512")
    val destination_tables = List("abortion_postsvm_w_relevancy_20230727",
                                  "guncontrol_postsvm_w_relevancy_20230727",
                                  "evolution_postsvm_w_relevancy_20230727",
                                  "existenceofgod_postsvm_w_relevancy_20230727")
    val queries = List(List("abortion", "pro-life", "pro-choice"),
                       List("strict", "gun", "control"),
                       List("evolution", "natural", "mechanism", "intelligent", "design"),
                       List("atheist", "theist", "God", "exist"))

    val tables = source_tables.zip(destination_tables)
    val tables_and_queries = tables.zip(queries)
    val dh = new DeduplicationHelperMethods()
    val log_messages: ListBuffer[String] = ListBuffer()

    tables_and_queries.foreach(t_and_q => {
      //
      // First Create the Dataframe that will contain the warc_record_id, categories, content, and relevancy score
      //
      val source_table_name = t_and_q._1._1
      val dest_table_name = t_and_q._1._2
      val query_list = t_and_q._2
      val query = dh.formatQuery(query_list, "text")
      log_messages += s"Processing Table: ${source_tablespace}.${source_table_name} -> ${dest_tablespace}.${dest_table_name}"

      val rdd_raw = sc.cassandraTable(source_tablespace, source_table_name)

      val rdd = sc.cassandraTable(source_tablespace, source_table_name)
        .where("expr(lucene_"+source_table_name+"_idx, '{ query: [" + query + "] }')")

      /*
       * So we use orderBy to do a parallel sort on the Spark cluster but we limit the results to just the
       * warc_record_id and the lucene (relevance score) fields so that we don't eat up a lot of compute cycles
       * moving the actual content around which would take hours.
       *
       * It is important to do the dataframe conversion first, and then use orderBy. sortBy on the RDD itself is not
       * lazy and you end up running the whole sort twice, once when it is called and then again when you create the
       * Dataframe, and the sort takes a significant amount of time. If you do it in the order below everything is
       * lazy and will only be computed once.
       *
       * Because this is just a data copy with the added relevance values from Lucene we don't want to filter
       * or limit the number of documents.
       */
      val relevance_df = rdd
        .map(x => Tuple2(x.getString("warc_record_id"), x.getFloat("lucene"))) // get the ID and relevance (not content!!)
        .toDF("warc_record_id", "lucene") // create the dataframe with two columns
        .orderBy(desc("lucene")) // now do the global sort to order by relevance
      relevance_df.cache() // cache the results so that subsequent operations are fast (the above as 10 to 20 min on 10 million docs)
      val rel_count = relevance_df.count()  // force Spark to run this now, it takes a long time but returns a small result, which is then cached
                            // If you don't force it now it will end up being run for the first time in the context of the
                            // withColumnRenamed call below and then times out as it is then wrapped in a future with a timeout of a
                            // couple of minutes and this can run for upwards of 20.
                            // Timeout Error looks like: java.util.concurrent.TimeoutException: Futures timed out after [300 seconds]

      log_messages += s"  Cassandra Size before query: ${rdd_raw.count()}, " +
        s"Size After Query ${rdd.count()}, Size with Relevance ${rel_count}."

      // Now get the content for the thousand relevant documents and join it to the dataframes

      // Snag the warc_ids from the dataframe
      val warc_ids = relevance_df.map(r => r.getString(0)).rdd
      // Create a new dataframe that has the content for the specific warc_record_ids that are relevant and join
      // See https://github.com/datastax/spark-cassandra-connector/blob/master/doc/2_loading.md#repartitioning-rdds-based-on-a-cassandra-tables-replication
      // for what the repartition call is doing.
      val contentRDD = warc_ids.map(Tuple1(_))  // the keys must be in a tuple to be used with Cassandra
        .repartitionByCassandraReplica("pilotparse", "wetrecord_refiltered_prepared")
        .joinWithCassandraTable("pilotparse", "wetrecord_refiltered_prepared")
        .map(x => Tuple3(x._2.getString("warc_record_id"), x._2.getString("categories"), x._2.getString("content")))
      // Create a dataframe with the content
      val content_df = contentRDD.toDF("warc_record_id", "categories", "content")
      // Join the content to the relevancy scores
      val full_df = relevance_df.join(content_df, "warc_record_id")
      // Rename the columns
      val df = full_df
        .withColumnRenamed("lucene", "relevance")
        .withColumnRenamed("content", "text")

      df.count()
      log_messages += s"   Records to Write: ${df.count()}"
      // Now write that Dataframe to the MySQL Database
      //df.write.mode(SaveMode.Append).jdbc(dbUrl, dest_table_name, dbProperties)
      df.write.jdbc(dbUrl, dest_table_name, dbProperties)
    })

    // Print out all of the log messages
    log_messages.toList.foreach(x => {
      println(x)
    })
    print("\n\n>>>>>> END OF PROGRAM <<<<<<\n\n")
  }
}