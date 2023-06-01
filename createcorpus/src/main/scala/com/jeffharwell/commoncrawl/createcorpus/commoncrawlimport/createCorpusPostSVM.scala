package com.jeffharwell.commoncrawl.createcorpus.commoncrawlimport

import com.datastax.spark.connector.{toRDDFunctions, toSparkContextFunctions}
import org.apache.spark.ml.feature.MinHashLSH
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.cassandra._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer
import scala.util.hashing.MurmurHash3

/*
## Overview

This code is based off of the search_and_dedup_20230503 Zeppelin notebook. Given set of search terms the code will
pull and deduplicate a list of results and write those back to Cassandra tables.

## Full Pipeline - Lucene Search to final corpus

Final corpus should include warc_record_id, relevance, text.

1. Get our list of documents from Lucene / Cassandra ("relevance", "warc_record_id", "text") sorted by relevance
2. Shingle all the documents and discard those with less than 0 shingles (currently at least one shingle must be created)
3. Run MinHashLSA and get the vector
4. Group the documents by the number of shared hashes they have
5. Create a list of warc_ids that we discarded because they were too similar to other documents but not the highest
   relevance similar document
6. Filter the original dataframe for documents not in the discard set to get our final corpus of documents
7. Export the results back to Cassandra

#### Deduplication

Steps:

1. For each document in the result list perform 6-shingling using the shingle_stopwords and murmurhash3
2. Use MinHashLSA to compute 32 min-hash values for each document
3. create an inverted index (hash -> documents)
4. For each document that shares a hash, estimate the similarity as a intersection of the hash tables for that document
5. If a document reaches the similarity threshold, pick the one with the highest relevance
6. The resulting document set, with relevancy scores, is the corpus to be used with the VDD algorithm.

Interestingly Broder uses 4-shingling, Theobald uses (I believe) d = 2 (two tokens between shingles) c = 3 (shingles
of length 3) k=6 (so 6 shingles per document) and l=32 (so 32 min-hash functions). They used a high value of l to
increase recall (which I think means being able to identify all possible duplicates) in their evaluation, uses the
same spot signatures for both LSH-S and SpotSigs (which I think means they used the same stopword list ...
but I'm not completely sure.)

    LSH can be tuned by concatenating k signatures from
    each data object into a single hash value for high precision,
    and by combining matches over l such hashing steps—using
    independent hash functions—for good recall. (Theobald et al. 2008 p. 3)

It is unclear to me from the paper how they pick the 6 signatures to concatenate. Broder used comparatively
fewer hashes and more signatures.

#### Shingle Stopwords

Theobald et al. reports the best SpotSig performance (F1) from "a combination of articles and flexions of the
verbs _be, can, will, have, do_" those have been defined below in the variable "shingle_stopwords".
 */


object createCorpusPostSVM extends java.io.Serializable {

  // UDF Functions
  def shingle(tokens: List[String], shingle_size: Int): List[Int] = {
    /*
     * Create a list of shingles from a list of tokens.
     */
    val shingles: ListBuffer[Int] = new ListBuffer[Int]()
    val murmurseed = 0
    var shingle_stopwords = List[String]("the", "a", "an",
      "be", "is", "are", "am", "was", "were", "being", "been", // Oxford Languages
      "can", "could", // merriam-webster
      "will", "would", // mirriam-webster
      "have", "had", "having", "has", // mirriam-webster
      "do", "did", "done", "doing", "does" // mirriam-webster
    )

    def getShingle(tokens: List[String]): Int = {
      val shingleList: List[String] = tokens.slice(0, shingle_size)
      MurmurHash3.listHash(shingleList, murmurseed)
    }

    @scala.annotation.tailrec
    def processTokens(tokens: List[String], shingles: ListBuffer[Int]): ListBuffer[Int] = {
      /* Recursive function that iterates through the list of tokens
       * and, when it finds a stop word, passes the stop word and the next shingle_size - 1
       * number of tokens to MurmurHash to create a shingle.
       */
      if (tokens.size < shingle_size) {
        shingles
      } else {
        if (shingle_stopwords.contains(tokens.head.toLowerCase())) {
          val shingleList: List[String] = tokens.slice(0, shingle_size)
          //println(shingleList.mkString(" "))
          shingles.append(MurmurHash3.listHash(shingleList, murmurseed))
        }
        //processTokens(tokens.tail, shingles)
        processTokens(tokens.slice(2, tokens.size), shingles)
      }
    }

    processTokens(tokens, shingles).toList.distinct
  }

  def shingle_six(content: String): List[Int] = {
    val tokens: List[String] = content.split(" ").toList
    /*
     * Create a list of shingles from a list of tokens.
     */
    val shingle_size = 6
    val shingles: ListBuffer[Int] = new ListBuffer[Int]()
    val murmurseed = 0
    var shingle_stopwords = List[String]("the", "a", "an",
      "be", "is", "are", "am", "was", "were", "being", "been", // Oxford Languages
      "can", "could", // merriam-webster
      "will", "would", // mirriam-webster
      "have", "had", "having", "has", // mirriam-webster
      "do", "did", "done", "doing", "does" // mirriam-webster
    )

    def getShingle(tokens: List[String]): Int = {
      val shingleList: List[String] = tokens.slice(0, shingle_size)
      MurmurHash3.listHash(shingleList, murmurseed)
    }

    @scala.annotation.tailrec
    def processTokens(tokens: List[String], shingles: ListBuffer[Int]): ListBuffer[Int] = {
      /* Recursive function that iterates through the list of tokens
       * and, when it finds a stop word, passes the stop word and the next shingle_size - 1
       * number of tokens to MurmurHash to create a shingle.
       */
      if (tokens.size < shingle_size) {
        shingles
      } else {
        if (shingle_stopwords.contains(tokens.head.toLowerCase())) {
          val shingleList: List[String] = tokens.slice(0, shingle_size)
          //println(shingleList.mkString(" "))
          shingles.append(MurmurHash3.listHash(shingleList, murmurseed))
        }
        //processTokens(tokens.tail, shingles)
        processTokens(tokens.slice(2, tokens.size), shingles)
      }
    }

    processTokens(tokens, shingles).toList.distinct
  }

  def shingle_udf: UserDefinedFunction = udf(shingle_six _)

  def aggregateHashesFunc(id: String, hashes: collection.mutable.WrappedArray[org.apache.spark.ml.linalg.DenseVector]): collection.mutable.WrappedArray[Double] = {
    // The hash is in a vector of length 1, so x(0) gets the hash
    hashes.map(x => x(0))
  }

  val aggregateHashes: UserDefinedFunction = udf(aggregateHashesFunc _)

  def sortSetFunc(s: collection.mutable.WrappedArray[String]): collection.mutable.WrappedArray[String] = {
    s.sorted
  }

  val sortSet: UserDefinedFunction = udf(sortSetFunc _)

  def calculateSimilarityFunc(a: Int, num_hashes: Int): Double = {
    // Estimate the similarity of each set of documents by the number of minhash collisions
    // We usually use num_hashes = 64
    a.toDouble / num_hashes
  }

  val calculateSimilarity: UserDefinedFunction = udf(calculateSimilarityFunc _)

  def getInDiscardUdf(documents_to_exclude: Set[String]): UserDefinedFunction = {
    // Create the closure
    // Note that documents_to_exclude must be a broadcasted variable, or the UDF
    // will fail with a null pointer exception.
    def inDiscard(id: String): Boolean = {
      documents_to_exclude.contains(id)
    }

    def inDiscardUdf: UserDefinedFunction = udf(inDiscard _)
    inDiscardUdf
  }



  def getNotInDiscardUdf(documents_to_exclude: Set[String]): UserDefinedFunction = {
    // Here is our closure
    // Note that documents_to_exclude must be a broadcasted variable, or the UDF
    // will fail with a null pointer exception.
    def notInDiscard(id: String): Boolean = {
      !documents_to_exclude.contains(id)
    }

    // Now create the UDF and return it
    def notInDiscardUdf: UserDefinedFunction = udf(notInDiscard _)
    notInDiscardUdf
  }


  def mostRelevant(ids: Seq[String], warc_record_id_to_relevance: Map[String, Float]): (String, Seq[String]) = {
    // Annoyingly Terse
    // warc_record_id_to_relevance maps each warc_record_id to its corresponding
    // relevance score in the search.
    // Go through our ids and create tuples of the id and the relevance
    // then we sort the whole structure by relevance (that is the _._2)
    // then we take the first element, which is going to be the one with the
    // highest relevance
    // then we take the first element of the tuple, which is going to be
    // the id that has the highest relevance.
    val sorted = ids.map(x => (x, warc_record_id_to_relevance(x))).sortWith(_._2 > _._2)
    val most_relevant = sorted.head._1
    val discarded = sorted.tail.map(x => x._1)
    (most_relevant, discarded)
  }

  def main(args: Array[String]): Unit = {
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
    println("Creating Topical Corpus PostSVM")
    println("Running on Spark version: " + sc.version)

    // We need to ignore any nulls, we are never inserting old data so we don't need to overwrite,
    // and we don't want to insert tombstones by inserting null values, as this causes problems with
    // performance that require compaction, which causes problems with disk space.
    // https://github.com/datastax/spark-cassandra-connector/blob/master/doc/5_saving.md
    // val ignoreNullsWriteConf = WriteConf.fromSparkConf(sc.getConf).copy(ignoreNulls = true)

    /*
    val target_tablespace = "cc_exports"
    val tables = List("abortion_corpus_20230503",
      "guncontrol_corpus_20230503",
      "evolution_corpus_20230503",
      "existenceofgod_corpus_20230503")
     */
    val source_tablespace = "cc_exports"  // tablespace in which to find the source tables
    val dest_tablespace = "cc_exports"  // tablespace in which to find the target tables
    val source_tables = List("abortion_postsvm_20230512",
                      "guncontrol_postsvm_20230512",
                      "evolution_postsvm_20230512",
                      "existenceofgod_postsvm_20230512")
    val destination_tables = List("abortion_corpus_20230512",
                                  "guncontrol_corpus_20230512",
                                  "evolution_corpus_20230512",
                                  "existenceofgod_corpus_20230512")
    val queries = List(List("abortion", "pro-life", "pro-choice"),
                       List("strict", "gun", "control"),
                       List("evolution", "natural", "mechanism", "intelligent", "design"),
                       List("atheist", "theist", "God", "exist"))
    val number_of_raw_documents = 1100  // the number of raw documents to collect per query
                                        // these documents are then de-duplicated
    val small_shingle_threshold = 0  // minimum number of generated shingles to require, evaluated on <=. So zero means
                                     // include the document if it has even a single shingle to it's name
    val tables = source_tables.zip(destination_tables)
    val tables_and_queries = tables.zip(queries)
    val dh = new DeduplicationHelperMethods()
    val log_messages: ListBuffer[String] = ListBuffer()

    tables_and_queries.foreach(t_and_q => {
      //
      // First Create the Dataframe that will contain the warc_record_id, categories, content, and relevancy score
      //
      val relevance_threshold = .1
      val source_table_name = t_and_q._1._1
      val dest_table_name = t_and_q._1._2
      val query_list = t_and_q._2
      val query = dh.formatQuery(query_list, "text")
      log_messages += s"Processing Table: ${source_tablespace}.${source_table_name} -> ${dest_tablespace}.${dest_table_name}"

      //val rdd = sc.cassandraTable("pilotparse","wetrecord_refiltered_prepared")
      //  .where("expr(lucene_wetrecord_refiltered_prepared_idx, '{ query: ["+query+"] }')")
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
       */
      val relevance_df = rdd
        .map(x => Tuple2(x.getString("warc_record_id"), x.getFloat("lucene"))) // get the ID and relevance (not content!!)
        .filter(x => x._2 > .1) // we only want records with the relevance greater than .1 (i.e. don't sort everything)
        .toDF("warc_record_id", "lucene") // create the dataframe with two columns
        .orderBy(desc("lucene")) // now do the global sort to order by relevance
        .limit(number_of_raw_documents) // only take the 1000 most relevant documents
      relevance_df.cache() // cache the results so that subsequent operations are fast (the above as 10 to 20 min on 10 million docs)
      relevance_df.count()  // force Spark to run this now, it takes a long time but returns a small result, which is then cached
                            // If you don't force it now it will end up being run for the first time in the context of the
                            // withColumnRenamed call below and then times out as it is then wrapped in a future with a timeout of a
                            // couple of minutes and this can run for upwards of 20.
                            // Timeout Error looks like: java.util.concurrent.TimeoutException: Futures timed out after [300 seconds]

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

      //
      // Now deduplicate that dataframe
      //


      // Create shingles from the text of each column
      val df_all_shingles = df.withColumn("shingles", shingle_udf(col("text")))
      // Create the Murmur3 hash shingles from each cell of text, filter out the rows that have 10 or less shingles
      // In the abortion dataset this only includes 90 document
      // You must filter out the documents with no shingles or they will fail in the MinHashLSA transform.
      //println(s"Number of documents with ${small_shingle_threshold} or less shingles ${df_all_shingles.filter(size(col("shingles")) <= small_shingle_threshold).count()}")
      //println(s"Number of documents with more shingle ${df_all_shingles.filter(size(col("shingles")) > small_shingle_threshold).count()}")
      log_messages += s"Number of documents with ${small_shingle_threshold} or less shingles ${df_all_shingles.filter(size(col("shingles")) <= small_shingle_threshold).count()}"
      log_messages += s"Number of documents with more shingle ${df_all_shingles.filter(size(col("shingles")) > small_shingle_threshold).count()}"
      val df_with_shingles = df_all_shingles.filter(size(col("shingles")) > small_shingle_threshold)
      // Collect all of the unique shingle values
      // don't do this!! Pulls everything through the RAM of the driver!!
      // val all_shingles: RDD[(String, Seq[Int])] = sc.parallelize(df_with_shingles.map(x => (x.getAs[String]("warc_record_id"), x.getAs[Seq[Int]]("shingles"))).collect())
      val all_shingles: RDD[(String, Seq[Int])] = df_with_shingles.map(x => (x.getAs[String]("warc_record_id"), x.getAs[Seq[Int]]("shingles"))).rdd

      val fs = dh.createFeatureSpaceFromShingles(all_shingles)
      val feature_space = fs._2
      // https://stackoverflow.com/questions/36290486/spark-job-restarted-after-showing-all-jobs-completed-and-then-fails-timeoutexce
      // feature_space.persist // maybe not needed?
      // We need to move this out of an RDD so that we can use it in other transforms (a wonder if there is a way around this)
      // so collect the RDD and cast it to a Map. Then broadcast it for speed.
      val feature_space_broadcast: org.apache.spark.broadcast.Broadcast[Map[Int, Long]] = sc.broadcast(feature_space.collect().toMap)
      val feature_space_size = fs._1
      // Map each document into the featurespace. We end up with a dataframe of sparse vectors representing the corpus.
      //var vectordata = text_sigs_rdd.map(x => toSparseVectorBroadcast(feature_space_size, x, feature_space_broadcast))
      val vectordata = all_shingles.map(x => dh.toSparseVectorBroadcast(feature_space_size, x, feature_space_broadcast))
      val vectordataframe = spark.createDataFrame(vectordata).toDF("id", "features")
      vectordataframe.persist

      // These documents are too short for us to consider (i.e., they didn't have enough stopword rich narrative to generate many shingles)
      //
      // Note, this makes a huge difference, it is a very effective was to filter out documents that are full of headlines or links and not much actual content.
      // This is important to note in the paper!!
      val short_documents_to_exclude = df_all_shingles.filter(size(col("shingles")) <= small_shingle_threshold).select("warc_record_id").collect().map(x => x.getAs[String]("warc_record_id")).toSet
      //print(f"\nWe are going to exclude ${short_documents_to_exclude.size} documents because they generated ${small_shingle_threshold} or less shingles.")
      log_messages += f"\nWe are going to exclude ${short_documents_to_exclude.size} documents because they generated ${small_shingle_threshold} or fewer shingles."

      // Run MinHashLSA
      //
      // Do the Min Hashing
      val mh = new MinHashLSH().setNumHashTables(64).setInputCol("features").setOutputCol("hashes")
      val model = mh.fit(vectordataframe)

      // Transformed is a DataFrame
      // Need to discern the structure and document it here, as it starts to get a bit obscure about here
      val transformed = model.transform(vectordataframe).cache

      //
      // Group the Documents by Shared MinHashes
      //
      //
      // Do the sorting and grouping to identify duplicates
      val all_minhash = transformed
      // Create a dataframe with just the id and the hash
      val new_df = all_minhash
        .withColumn("hash_array", aggregateHashes(col("id"), col("hashes")))
        .drop("hashes")
      val new_df2 = new_df
        .drop("features")
        .withColumn("each_hash", explode(col("hash_array")))
        .drop("hash_array")
      // Now aggregate by hash and sort the sets of texts
      val agg_df = new_df2
        .groupBy("each_hash")
        .agg(collect_list("id").as("texts"))
        .withColumn("sorted", sortSet(col("texts")))

      //
      // Estimate the Similarity and Discard Less Relevant Documents
      //

      val estimated_similarity = agg_df
        .filter(size(col("sorted")) > 1)
        .groupBy("sorted")
        .count()
        .withColumn("est_similarity", calculateSimilarity(col("count"), lit(64)))
      val filtered_for_similarity_df = estimated_similarity.filter(col("est_similarity") > .10)

      // Use map to get our (record id, relevance) tuple, pull it into the driver, then create the Map
      // This lives in the driver (taking up RAM ...)
      val warc_record_id_to_relevance = df
        .select("warc_record_id", "relevance")
        .map(x => (x.getString(0), x.getFloat(1)))
        .collect()
        .toMap

      // This is a very cool structure. The map allows us to apply arbitrary functions, including closures, to the rows
      // of our dataframe and output a new dataframe with new columns. Basically we are creating an anonomyous function
      // which is applied to each row of the dataframe and outputs a completely new dataframe.
      val most_relevant_from_duplicates = filtered_for_similarity_df.map(row => {
        // first get the most relevant id from the list
        val (most_relevant_id, discarded) = mostRelevant(row.getAs[Seq[String]](0), warc_record_id_to_relevance)
        (row.getAs[Seq[String]](0), row.getLong(1), row.getDouble(2), most_relevant_id, discarded)
      }).toDF("sorted", "count", "est_similarity", "most relevant", "discarded")

      // Ok, we are close. now we can create a flatmap of discarded warc ids.
      // It is important to remember that a document might appear in multiple collections of duplicate hashes, and it might
      // be the most relevant document in one group of duplicate hashes, but not the most relevant document in another. So this
      // step makes a complete list of all documents discarded from any set because once a document has been discarded we know
      // that another similar document exists that has higher search relevancy so there is no need to include it in the final
      // results even if it is the most relevant set from another set of duplicates.
      val discarded_set = most_relevant_from_duplicates
        .select("discarded")
        .collect()
        .map(x => x.getAs[Seq[String]](0))
        .flatMap(x => x)
        .toSet
      //print(f"\nWe are going to discard ${discarded_set.size} warc record ids from the ${df.count()} document retrieved in the search.")
      log_messages += f"\nWe are going to discard ${discarded_set.size} duplicate warc record ids from the ${df.count()} document retrieved in the search."

      //
      // Exclude Duplicates and Documents with Few Shingles from the Result Set
      //

      // We want to exclude the duplicates as well as any document that generated two few shingles
      val documents_to_exclude: Set[String] = discarded_set | short_documents_to_exclude
      // Try this: https://stackoverflow.com/questions/40673773/how-to-use-a-broadcast-collection-in-a-udf
      // If we broadcast the variable, we can use it in a UDF, otherwise you get null pointer exceptions when you try
      // to execute it on the workers.
      // We must broadcast this variable so that it is available to the workers when they run their UDF functions.
      sc.broadcast(documents_to_exclude)
      //print(f"We are excluding ${documents_to_exclude.size} warc record ids due to duplication or not creating enough shingles")
      log_messages += f"We are excluding ${documents_to_exclude.size} warc record ids due to duplication or not creating enough shingles"

      // Trying to keep the closure tight so that we don't accidentally pull in the sc (which isn't serializable.
      // So this function creates a closure that has just the documents_to_exclude variable, the wraps it in a UDF and
      // returns it.
      val notInDiscardUdf: UserDefinedFunction = getNotInDiscardUdf(documents_to_exclude)

      val final_document_list = df.filter(notInDiscardUdf(col("warc_record_id")))

      //print(f"Initial Document Count ${df.count()} and deduped count ${final_document_list.count()}")
      log_messages += f"Initial Document Count ${df.count()} and deduped count ${final_document_list.count()}"

      //print(final_document_list.select("warc_record_id", "relevance").show(10))
      log_messages += final_document_list.select("warc_record_id", "relevance").show(2).toString

      //
      // Write back to Cassandra
      //
      final_document_list.write.cassandraFormat(dest_table_name, dest_tablespace).mode("append").save()

    })

    // Print out all of the log messages
    log_messages.toList.foreach(x => {
      println(x)
    })
    print("\n\n>>>>>> END OF PROGRAM <<<<<<\n\n")
  }
}