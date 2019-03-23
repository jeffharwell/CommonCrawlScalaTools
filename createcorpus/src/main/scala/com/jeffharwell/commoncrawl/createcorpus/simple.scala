package com.jeffharwell.commoncrawl.createcorpus

//import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object simple {
  def main(args: Array[String]) {
    //val sc =SparkSession.builder.appName("hello").getOrCreate()
    val conf = new SparkConf().setAppName("hello")
    val sc = new SparkContext(conf)
    sc.stop()
  }
}
