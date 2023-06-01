package com.jeffharwell.commoncrawl.createcorpus.commoncrawlimport

// The Cassandra Pieces
import org.apache.spark.sql.functions.udf

// Deduplication
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.rdd.RDD

import scala.util.hashing.MurmurHash3
import scala.collection.mutable.ListBuffer

import scala.collection.mutable

/*
 * Functions for the createCorpus object/process.
 */
class DeduplicationHelperMethods() extends java.io.Serializable {

  // And our Featurespace Creation Functions
  def createFeatureSpaceFromShingles(fingerprints: RDD[(String, Seq[Int])]): (Int, RDD[(Int, Long)]) = {
    // Cool, now we take our lists of shingles and create a single gigantic feature space that consists of all the unique shingles
    // the x => x is just the identity, so we just use flatMap to flatten the nested lists
    val featurespace = fingerprints.flatMap(x => x._2).distinct().cache
    // Create the index, the first entry is the fingerprint (the Int) the second is the index (the Long)
    var featurespace_index: RDD[(Int, Long)] = featurespace.zipWithIndex.cache

    // We need the size of the feature space for the sparse vectors
    var all_ones = featurespace.map(x => 1)
    var featurespace_size: Int = all_ones.reduce(_ + _)

    (featurespace_size, featurespace_index)
  }

  def toSparseVectorBroadcast(size: Int, t: (String, Seq[Int]), featurespace: org.apache.spark.broadcast.Broadcast[Map[Int, Long]]): (String, org.apache.spark.ml.linalg.Vector) = {
    val doc = t._1
    val features = t._2
    // You would think it would make the most sense to use the featurespace as a pairwiseRDD, but you can't because this function
    // itself is called within an RDD transform, and you can't manipulate an RDD from within an RDD transform, only from
    // within a Spark context.
    // So instead we use a spark Broadcast map, which means we need to use the .value() method to get the value for the key.
    //
    // Note that the sparse vector constructor will not take a Long as a value, it requires an Int, so do the conversion
    // of the index Long to an Int using the .toInt method within the transform.
    val features_by_index: Seq[(Int, scala.Double)] = t._2.map(x => (featurespace.value(x).toInt, 1.0)).distinct
    val vector: org.apache.spark.ml.linalg.Vector = org.apache.spark.ml.linalg.Vectors.sparse(size, features_by_index)
    (doc, vector)
  }

  def formatQuery(query_list: List[String], field_name: String = "content"): String = {
    var template = s"""{type: "contains", field: "${field_name}", values: ["%s"]}"""
    var templates: mutable.ListBuffer[String] = mutable.ListBuffer()
    query_list.foreach(x => {
      templates += template.format(x)
    })
    templates.mkString(",")
  }

}

/*
 * Create a companion object so that we can instantiate the class without needing the 'new' keyword.
 */
object DeduplicationHelperMethods {
  def apply() = new DeduplicationHelperMethods()
}