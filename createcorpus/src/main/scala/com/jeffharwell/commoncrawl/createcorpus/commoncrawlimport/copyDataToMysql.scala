package com.jeffharwell.commoncrawl.createcorpus.commoncrawlimport

import com.datastax.spark.connector.cql.{CassandraConnector, CassandraConnectorConf}
import com.datastax.spark.connector.toRDDFunctions
import com.datastax.spark.connector.writer.WriteConf
import com.jeffharwell.commoncrawl.warcparser.{FourForumsWARCStreamFilter, FourForumsWARCTopicFilter}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession


import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.collection.mutable.ListBuffer


object copyDataToMysql {
  def main(args: Array[String]) {

    print("\n\n>>>>>> START OF PROGRAM <<<<<<\n\n");

    // Check our arguments
    if (args.length == 0) {
      print("\n\n>>>>>>>>> ERROR: Must pass the username and password for the database an argument to spark-submit\n\n")
      print(">>>>>>>> EXITING <<<<<<<<<\n\n")
      throw new RuntimeException("no arguments pass to parseWETFilesFromCommonCrawl")
    }
    val database: String = args(0)
    val username: String = args(1)
    val password: String = args(2)

    print(f"${database} ${username} ${password}")

    // First set up the Spark context and point it to the Cassandra cluster.
    // The DNS name cassandra.default.svc.cluster.local resolves to the correct Cassandra
    // cluster within Kubernetes
    val conf = new SparkConf()
      .setAppName("copytomysql")
      .set("spark.cassandra.connection.host", "cassandra.default.svc.cluster.local")
    val sc = new SparkContext(conf)

    println("Running on Spark version: " + sc.version)

    val spark = SparkSession.builder.config(sc.getConf).getOrCreate()
    import spark.implicits._
    //val ignoreNullsWriteConf: WriteConf = WriteConf.fromSparkConf(sc.getConf).copy(ignoreNulls = true,
    //  consistencyLevel = ConsistencyLevel.ONE)

    println("Creating Dataframe")

    val sampleDF = Seq((1, "James", 30, "M"),
      (2, "Ann", 40, "F"), (3, "Jeff", 41, "M"),
      (4, "Jennifer", 20, "F")
    ).toDF("id", "name", "age", "gender")

    println("Writing Dataframe")
    println(f"Dataframe Length: ${sampleDF.count()}")
    println(f"JDBC URL: jdbc:mysql://mariadb.default.svc.cluster.local:3306/${database}")

    sampleDF.write
      .format("jdbc")
      .mode("overwrite")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("url", f"jdbc:mysql://mariadb.default.svc.cluster.local:3306/${database}")
      .option("dbtable", "sample_dataframe")
      .option("user", username)
      .option("password", password)
      .save()

    print("\n\n>>>>>> END OF PROGRAM <<<<<<\n\n");
  }
}