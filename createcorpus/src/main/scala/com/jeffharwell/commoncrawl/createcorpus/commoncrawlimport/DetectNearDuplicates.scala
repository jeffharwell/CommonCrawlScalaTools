package com.jeffharwell.commoncrawl.createcorpus.commoncrawlimport

import org.apache.spark.rdd.RDD
import org.apache.spark.sql
import org.apache.spark.sql.catalyst.dsl.expressions.{DslExpression, StringToAttributeConversionHelper}
import org.apache.spark.sql.expressions.UserDefinedFunction

import java.net.URL
import scala.collection.mutable

// Files, inputstreams, and gzipped data
import java.io.FileInputStream
import java.io.BufferedReader
import java.io.InputStreamReader
import java.util.zip.GZIPInputStream
import java.sql.Timestamp
import java.net.URL
import java.net.MalformedURLException

// Spark MLLib
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.linalg.SparseVector
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.linalg.SQLDataTypes.VectorType // alias for the VectorUDT type needed to define function that operation on vectors
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._ // needed for the dataframe transformations
import org.apache.spark.rdd.PairRDDFunctions // needed for the join
//import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry} // needed to create the Sparse Vector
import org.apache.spark.ml.feature.MinHashLSH

// Adds the converters .asScala so we can convert Java lists to Scala Lists
// https://stackoverflow.com/questions/16162090/how-to-convert-a-java-util-list-to-a-scala-list/16164163#16164163
import scala.collection.JavaConverters._

import scala.util.hashing.MurmurHash3
import scala.collection.mutable.ListBuffer
import edu.stanford.nlp.process.PTBTokenizer
import edu.stanford.nlp.process.CoreLabelTokenFactory
import java.io.StringReader

// The Cassandra Pieces
import com.datastax.spark.connector._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import com.datastax.driver.core.ConsistencyLevel
import com.datastax.spark.connector.writer._
import com.datastax.spark.connector.types.CassandraOption
import com.datastax.spark.connector.cql.CassandraConnector // needed to execute CQL against cassandra
import com.datastax.spark.connector.cql.CassandraConnectorConf

// when writing we want to ignore nulls and also use consistency level of ONE (we just need one host to confirm
// that is has the write).
// See: https://docs.datastax.com/en/cassandra/3.0/cassandra/dml/dmlConfigConsistency.html
//      https://docs.datastax.com/en/drivers/java/2.0/com/datastax/driver/core/ConsistencyLevel.html#ONE


// The Bind Class for persisting the feature space to Cassandra
case class FeatureSpaceBind(feature_index: Int, feature_value: Int)

class DetectNearDuplicates(sc_arg: SparkContext) {
  import org.apache.spark.implicits._

  val sc: SparkContext = sc_arg

  val ignoreNullsWriteConf: WriteConf = WriteConf.fromSparkConf(sc.getConf).copy(ignoreNulls = true, consistencyLevel = ConsistencyLevel.ONE)

  def processDocuments(table_name: String, load_feature_space: Boolean): Unit = {
    /*
     * Process a set of documents in a Cassandra table and
     */

    // Grab the golden standard records and create fingerprints
    var recordsrdd = sc.cassandraTable("pilotparse",table_name)
    var fingerprints = recordsrdd.map(x => (x.getString("warc_record_id"), shingle(tokenize(x.getString("content")), 9)))
    fingerprints.cache

    // Get the feature space, either load it from Cassandra or compute it directly
    val fs: (Int, RDD[(Int, Long)]) = {
      if (load_feature_space) {
        loadFeatureSpaceFromCassandra()
      } else {
        createFeatureSpaceFromDocuments(fingerprints, table_name: String, false)
      }
    }
    val feature_space = fs._2
    val feature_space_size = fs._1

    //Set the feature space as a broadcast variable so that it can be used by all of the worker nodes
    //later when we create the sparse vectors from the document fingerprints.
    //
    //I tried having each node do the lookups directly from Cassandra but it is much, much, slower than using the
    //broadcast variable. Obviously this broadcast method has some scalability problems. At 550 documents the
    //feature space is almost 1 million distinct shingles.
    var feature_space_broadcast = sc.broadcast(feature_space)

    // Map each document into the featurespace. We end up with a dataframe of sparse vectors representing the corpus.
    var vectordata = fingerprints.map(x => toSparseVectorBroadcast(feature_space_size, x, feature_space_broadcast))
    var vectordataframe = spark.createDataFrame(vectordata).toDF("id", "features")
    vectordataframe.persist

    // Do the Min Hashing
    val mh = new MinHashLSH().setNumHashTables(5).setInputCol("features").setOutputCol("hashes")
    val model = mh.fit(vectordataframe)

    // Transformed is a DataFrame
    // Need to discern the structure and document it here, as it starts to get a bit obscure about here
    var transformed = model.transform(vectordataframe).cache

    /*
    transformed.withColumn("minhash", minOfHash(col("hashes"))).show
    +--------------------+--------------------+--------------------+--------------+
    |                  id|            features|              hashes|       minhash|
    +--------------------+--------------------+--------------------+--------------+
    |<urn:uuid:784bfc8...|(963519,[23,921,3...|[[-2.037823539E9]...|-2.037823539E9|
    |<urn:uuid:21a223b...|(963519,[236,4532...|[[-2.021606019E9]...|-2.037729126E9|
    |<urn:uuid:a5acd30...|(963519,[211,384,...|[[-2.03801839E9],...| -2.03801839E9|
     */

    /*
    This will dump the documents with their min hash, if you were in Zeppelin
    z.show(transformed.withColumn("minhash", minOfHash(col("hashes"))).select("id","minhash"))
     */

    /*
     Nifty, transform the dataframe so that we can see the count of each minhash, this should result in grouping documents into buckets where all of the similar
     documents end up in the same bucket.
     z.show(transformed.withColumn("minhash", minOfHash(col("hashes"))).select("id","minhash").groupBy("minhash").agg(count($"minhash")).filter($"count(minhash)" > 1))
     */
    val minhash_list: List[Double] = transformed.withColumn("minhash", minOfHash(col("hashes")))
      .select("id","minhash").groupBy("minhash")
      .agg(count($"minhash")).filter($"count(minhash)" > 1)
      .select("minhash").map(f=>f.getDouble(0)).collect.toList

    minhash_list.foreach { x: Double =>
      saveDuplicateIDs(transformed, x)
    }

    /* Extract Only Dissimilar Documents
    Rough Algorithm
    1. create a dataframe with uid, relevance, and minhash
    2. transform that into dataframe with one row for each minhash and Array((uid, relevance),) as an column
    3. In another transformation sort the array by relevance and take the head
    4. You should end up with a dataframe that has rows of minhash, (uid, relevance). Do a foreach over that dataframe,
        grab the record out of golden_politics for that specific uid and write it into golden_politics_deduped
     */

    // Step 1

    // Dataframe with just the uid and the min of the hashes from LSH algorithm
    var id_and_hash_df = transformed.withColumn("minhash", minOfHash(col("hashes"))).select("id","minhash")

    // Get the uids and relevance as a dataframe
    //
    var id_and_relevance_rdd = sc.cassandraTable("pilotparse",table_name).map(x => (x.getString("warc_record_id"), x.getString("relevance")))
    var id_and_relevance_df = id_and_relevance_rdd.toDF("id", "relevance")
    id_and_relevance_df.schema

    // Do the join
    // From the ever helpful stack overflow: https://stackoverflow.com/questions/40343625/joining-spark-dataframes-on-the-key/40356825
    var minhash_relevance = id_and_hash_df.join(id_and_relevance_df,Seq("id")).cache

    var minhash_relevance_combined = minhash_relevance.withColumn("uid_relevance", array(col("id"), col("relevance"))).cache

    // Step 2

    // https://stackoverflow.com/questions/34202997/how-to-aggregate-values-into-collection-after-groupby
    val flatten = udf((xs: Seq[Seq[String]]) => xs.flatten)
    //val collectdata = collect_list($"id", $"relevance")
    val collectdata = collect_list($"uid_relevance")

    // Explicitly create the data structure a cache it
    // rows arew minhash, WrappedArray(WrappedArray(uid, relevence))
    // I hear you like WrappedArrays, so I put a WrappedArray, in your WrappedArray ... :(
    var aggregated_data = minhash_relevance_combined.groupBy($"minhash").agg(collectdata).cache

    // Step 3 Sort the arrays by relevance and then take the head element

    //
    // A UDF which will sort the wrapped array in order of descending relevance and then
    // grab the most relevant uid, which is now at the front of the array.
    //
    var getMostRelevantUid = udf((x: mutable.WrappedArray[mutable.WrappedArray[String]]) => {
      // Sort by relevance descending
      var sorted = x.sortWith(_.apply(1) > _.apply(1))
      // Grab the uid of the first element in the sorted list
      sorted.head.apply(0)
    })

    // z.show(aggregated_data.withColumn("mostrelevantuid", getMostRelevantUid($"collect_list(uid_relevance)")))

    // Step 4: Write the full record for each most relevant uid to Cassandra

    // More Good Times
    def writeSingleToCassandraDeduped(uid: String): Unit = {
      println(uid)
      // I'm a bit surprised that this works but, since the duplicates and golden_politics tables share the same schema
      // apparently we can directly write the results of one to the other without going through a bind class
      sc.cassandraTable("pilotparse","golden_politics").where(s"warc_record_id = '${uid}'").saveToCassandra("pilotparse","golden_politics_deduped")
    }

    //
    // Select our list of most revelant uids, grab the full record out of golden_politics, and write it into golden_politics_deduped
    //
    aggregated_data.withColumn("mostrelevantuid", getMostRelevantUid($"collect_list(uid_relevance)")).select("mostrelevantuid").rdd.collect.foreach{ x =>
      // We end up with x: org.apache.spark.sql.Row = [<urn:uuid:f2647526-9752-45c2-8fe6-04abdbdab4f5>]
      // so unwrap it and convert the Any type to a String
      writeSingleToCassandraDeduped(x(0).toString)
    }

  }



  /*
   * Methods
   */

  // Bit ugly, within the dataframe of the transformed dataset the x hashes generated by the LSH algorithm are a WrappedArray of single element vectors.
  // This udf converts that to something that we can work with, an Array of Doubles, and then grabs the minimum. According the the LSH proof the documents
  // that share the same min hash are likely similar.
  var minOfHash: UserDefinedFunction = udf((v: mutable.WrappedArray[org.apache.spark.ml.linalg.Vector]) => {
    v.map(x => x(0)).toArray.min
  })

  def saveDuplicateIDs(transformed: sql.DataFrame, minhash_value: Double): Unit =  {
    /*
     * Name of the game is to be able to select all the documents that have the same hash and put them in a specific table on
     * Cassandra so that we can take a look at them.
     */
    var id_minhash = transformed.withColumn("minhash", minOfHash(col("hashes"))).cache
    var duplicate_ds = id_minhash.filter("minhash = -2037619142").select("id").map(x => x.getAs[String]("id"))

    // Good Times
    def writeSingleToCassandra(uid: String): Unit = {
      println(uid)
      // I'm a bit surprised that this works but, since the duplicates and golden_politics tables share the same schema
      // apparently we can directly write the results of one to the other without going through a bind class
      sc.cassandraTable("pilotparse","golden_politics").where(s"warc_record_id = '${uid}'").saveToCassandra("pilotparse","duplicates")
    }

    duplicate_ds.rdd.collect.foreach{ x => writeSingleToCassandra(x) }
  }

  def toSparseVectorBroadcast(size: Int, t: (String, List[Int]), featurebroadcast: org.apache.spark.broadcast.Broadcast[scala.collection.Map[Int,Int]]): (String, org.apache.spark.ml.linalg.Vector) = {
    val doc = t._1
    val features = t._2
    val features_by_index = t._2.map(x => (featurebroadcast.value(x),1.0)).distinct
    val vector: org.apache.spark.ml.linalg.Vector = org.apache.spark.ml.linalg.Vectors.sparse(size, features_by_index)
    (doc, vector)
  }

  def createFeatureSpaceFromDocuments(fingerprints: RDD[(String, List[Int])], table_name: String, persist: Boolean): (Int, RDD[(Int, Long)]) = {
    // Cool, now we take our lists of shingles and create a single gigantic feature space that consists of all the unique shingles
    // the x => x is just the identity, so we just use flatMap to flatten the nested lists
    val featurespace = fingerprints.flatMap(x => x._2).distinct().cache
    // Create the index
    var featurespace_index = featurespace.zipWithIndex.cache

    // We need the size of the feature space for the sparse vectors
    var all_ones = featurespace.map(x => 1)
    var featurespace_size: Int = all_ones.reduce(_ + _)

    if (persist) {
      persistFeatureSpace(featurespace_index)
    }

    (featurespace_size, featurespace_index)
  }

  def loadFeatureSpaceFromCassandra(): (Int, RDD[(Int, Long)]) = {
    //
    // Snag the featurespace from Cassandra
    var featurespace = sc.cassandraTable("pilotparse","feature_space").map{x => (x.get[Int]("feature_value"), x.get[Long]("feature_index"))}

    // Calculate the size of the featurespace
    var all_ones = featurespace.map(x => 1)
    var featurespace_size: Int = all_ones.reduce(_ + _)

    // This seems an unfortunate way to do this as you have to collect the entire feature space into the RAM
    // of the driver just to count it.
    // var featurepairrdd: org.apache.spark.rdd.PairRDDFunctions[Int, Long] = featurespace
    // var featurespace_map = featurepairrdd.collectAsMap
    // var featurespace_size: Int = featurespace_map.size

    (featurespace_size, featurespace)
  }

  def persistFeatureSpace(featurespace_index: RDD[(Int, Long)]): Unit = {
    /*
     * Save the feature space into a table in Cassandra
     */
    var fsrdd = featurespace_index.map{x => FeatureSpaceBind(x._2.toInt, x._1)}
    fsrdd.saveToCassandra("pilotparse", "feature_space")
  }

  def shingle(tokens: List[String], shingle_size: Int): List[Int] = {
    /*
     * Create a list of shingles from a list of tokens.
     */
    val shingles: ListBuffer[Int] = new ListBuffer[Int]()
    val murmurseed = 0
    // https://github.com/mitmedialab/DataBasic/blob/master/nltk_data/corpora/stopwords/english
    val python_stop_words = List("i","me","my","myself","we","our","ours","ourselves","you","your","yours","yourself","yourselves","he","him",
      "his","himself","she","her","hers","herself","it","its","itself","they","them","their","theirs","themselves",
      "what","which","who","whom","this","that","these","those","am","is","are","was","were","be","been","being","have",
      "has","had","having","do","does","did","doing","a","an","the","and","but","if","or","because","as","until","while",
      "of","at","by","for","with","about","against","between","into","through","during","before","after","above","below",
      "to","from","up","down","in","out","on","off","over","under","again","further","then","once","here","there","when",
      "where","why","how","all","any","both","each","few","more","most","other","some","such","no","nor","not","only",
      "own","same","so","than","too","very","s","t","can","will","just","don","should","now")

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
        if (python_stop_words.contains(tokens.head.toLowerCase())) {
          val shingleList: List[String] = tokens.slice(0, shingle_size)
          println(shingleList.mkString(" "))
          shingles.append(MurmurHash3.listHash(shingleList, murmurseed))
        }
        processTokens(tokens.tail, shingles)
      }
    }
    processTokens(tokens, shingles).toList.distinct
  }

  def tokenize(s: String): List[String] = {
    /*
     * Tokenize a string using the Stanford PTBTokenizer
     */
    var line_as_reader = new StringReader(s)
    var ptbt = new PTBTokenizer(line_as_reader, new CoreLabelTokenFactory(), "")
    var tokens = new ListBuffer[String]()
    while (ptbt.hasNext()) {
      var label = ptbt.next()
      tokens += label.originalText() //edu.stanford.nlp.ling.CoreLabel
    }
    tokens.toList
  }
}
