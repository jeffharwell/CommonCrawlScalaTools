package com.jeffharwell.commoncrawl.createcorpus

//import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import com.jeffharwell.commoncrawl.warcparser.Parser
import com.jeffharwell.commoncrawl.warcparser.WARCRecord
import com.jeffharwell.commoncrawl.warcparser.MyWARCFilter
import com.jeffharwell.commoncrawl.warcparser.MyWARCCategorizer

object hello {
  def main(args: Array[String]) {
    //val sc =SparkSession.builder.appName("hello").getOrCreate()
    val conf = new SparkConf().setAppName("hello")
    val sc = new SparkContext(conf)
    print("\n\n>>>>>> START OF PROGRAM <<<<<<\n\n");
    println("Hello World.");
    println("Running on Spark version: "+sc.version)
    print("\n\n>>>>>> END OF PROGRAM <<<<<<\n\n");

    val c: MyWARCCategorizer = new MyWARCCategorizer(2)
  }
}
