package com.jeffharwell.commoncrawl.createcorpus.commoncrawlimport

import com.datastax.driver.core.ConsistencyLevel
import com.datastax.spark.connector.cql.CassandraConnectorConf
import com.datastax.spark.connector.toRDDFunctions
import com.datastax.spark.connector.writer.WriteConf
import com.jeffharwell.commoncrawl.warcparser.FourForumsWARCTopicFilter
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

object parseWETFilesFromCommoncrawl {
  def main(args: Array[String]) {

    // Check our arguments
    if (args.length == 0) {
      print("\n\n>>>>>>>>> ERROR: Must pass the number of wet paths to select randomly as an argument to spark-submit\n\n")
      print(">>>>>>>> EXITING <<<<<<<<<\n\n")
      throw new RuntimeException("no arguments pass to parseWETFilesFromCommonCrawl")
    }
    val s_number_of_wet_paths: String = args(0)

    val number_of_wet_paths = {
      try {
        s_number_of_wet_paths.toInt
      } catch {
        case e: NumberFormatException => {
          print("\n\n>>>>>>>>> ERROR: Must pass the number of wet paths to select randomly as an argument to spark-submit\n\n")
          print(s"$s_number_of_wet_paths is not a valid number")
          print(">>>>>>>> EXITING <<<<<<<<<\n\n")
          throw new RuntimeException("invalid arguments passed to parseWETFilesFromCommonCrawl")
        }
      }
    }

    // First set up the Spark context and point it to the Cassandra cluster.
    // The DNS name cassandra.default.svc.cluster.local resolves to the correct Cassandra
    // cluster within Kubernetes
    val conf = new SparkConf()
      .setAppName("importcommoncrawl")
      .set("spark.cassandra.connection.host", "cassandra.default.svc.cluster.local")
    val sc = new SparkContext(conf)
    //val ignoreNullsWriteConf: WriteConf = WriteConf.fromSparkConf(sc.getConf).copy(ignoreNulls = true,
    //  consistencyLevel = ConsistencyLevel.ONE)

    print("\n\n>>>>>> START OF PROGRAM <<<<<<\n\n");
    println(s"Parsing $s_number_of_wet_paths Random WET Files from Common Crawl");
    println("Running on Spark version: " + sc.version)

    val cc_url: String = "https://data.commoncrawl.org"

    // Get a few random wetpaths that we have not processed yet
    val getwetpaths = new GetWETPaths(sc)
    val r = getwetpaths.getRandomWETPaths(number_of_wet_paths)
    println(s"Retrieved ${r.length.toString} paths")

    // Set Up our URLs - we want to create an RDD that has the URLs we
    // want to process in partitions so that we can send them out to the
    // worker nodes.
    var url = ListBuffer[String]()
    r.map(x => url += x._1)

    var length = url.size

    var urls = {
      if (length <= 27) {
        sc.parallelize(url)
      } else {
        // We want about 3 WET Archive files per partition
        sc.parallelize(url, length / 3)
      }
    }

    // Be CAREFUL! The SparkContext (the sc) can never touch the RDD in any way or you will get a very difficult to
    // troubleshoot NPE. This means that if you put the sc in any function that gets passed to the workers, or inside
    // a map that is called on an RDD, then, when the statement finally runs on the works it will fail with a Null
    // Pointer Exception
    //
    // So, as in this case, utilize the sc outside of the RDD and just pass in the final product, in this case
    // the CassandraConf
    //
    // So the ProcessWETPaths object is going to be serialized and sent out to each worker so that it can
    // process each url in its partition. So ProcessWETPaths must be serializable and can't depend directly on
    // the spark context.
    println("\n\n>> Configuring FourForumnsWARCTopicFilter as topic filter.")
    val ffc: FourForumsWARCTopicFilter = new FourForumsWARCTopicFilter()
    println("\n\n>> Configuring Thresholds for topic filter")
    ffc.setMentions("abortion", Map("core" -> 1, "secondary" -> 1))
    ffc.setMentions("evolution", Map("core" -> 1, "secondary" -> 2))
    ffc.setMentions("existenceofgod", Map("core" -> 1, "secondary" -> 1))
    ffc.setMentions("guncontrol", Map("core" -> 1, "secondary" -> 1))

    val cassandraconf = CassandraConnectorConf(sc.getConf)
    val dcc = new ProcessWETPaths(cc_url, sc.getConf)
    val parsed_records = urls.map(x => dcc.parseWETArchiveURL(x, cassandraconf, ffc))

    // Now do it, parse the records
    //parsed_records.flatMap(identity).saveToCassandra("pilotparse", "wetrecord", writeConf = ignoreNullsWriteConf)
    parsed_records.flatMap(identity).saveToCassandra("pilotparse", "wetrecord")

    print("\n\n>>>>>> END OF PROGRAM <<<<<<\n\n");
  }
}