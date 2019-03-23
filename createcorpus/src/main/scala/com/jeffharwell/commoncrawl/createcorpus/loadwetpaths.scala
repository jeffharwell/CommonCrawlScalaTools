package com.jeffharwell.commoncrawl.createcorpus

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import com.datastax.spark.connector._
import com.datastax.spark.connector.writer._
import com.datastax.spark.connector.cql.CassandraConnector // needed to execute CQL against cassandra
import com.datastax.driver.core.ResultSet
import com.datastax.driver.core.Row

//import java.io.BufferedInputStream
//import java.io.FileInputStream
//import java.io.File
import java.net.URL

//import java.io.FileInputStream
//import java.io.BufferedReader
//import java.io.InputStreamReader
import java.util.zip.GZIPInputStream

/*
 *
 */

object loadwetpaths {
  def main(args: Array[String]) {
    print("\n\n>>>>>> START OF PROGRAM <<<<<<\n\n");

    // Define a few things
    // The DNS name cassandra.default.svc.cluster.local resolves to the correct Cassandra
    // cluster within Kubernetes
    val conf = new SparkConf()
      .setAppName("loadwetpaths")
      .set("spark.cassandra.connection.host", "cassandra.default.svc.cluster.local")
      //.set("spark.cassandra.connection.host", "k8master.fuller.edu")

    val sc = new SparkContext(conf)

    def processWETPathsLine(line: String): WETPathBind = {
        val filename = line.split("/")(5)
        new WETPathBind(Some(line), Some(filename), None, None, Some(false), Some(0), None)
    }

    println("Truncating pilotparse.wetpaths")
	CassandraConnector(sc.getConf).withSessionDo { session =>
	  session.execute("truncate table pilotparse.wetpaths")
	}

	// Queue up the file, we are streaming from a GZipped file on the web
	val url = new URL("https://commoncrawl.s3.amazonaws.com/crawl-data/CC-MAIN-2016-50/wet.paths.gz")
	val gzipInputStream = new GZIPInputStream(url.openStream())
	val lines = scala.io.Source.fromInputStream(gzipInputStream).getLines()

	// Process the lines, bind them to the WARCPathBind class which defines the Cassandra table schema, then write them.
	val wetpaths = lines.map {x => processWETPathsLine(x)}
	val wetpathsRDD = sc.parallelize(wetpaths.toList)
    val ignoreNullsWriteConf = WriteConf.fromSparkConf(sc.getConf).copy(ignoreNulls = true)
	wetpathsRDD.saveToCassandra("pilotparse", "wetpaths", writeConf = ignoreNullsWriteConf)

    // Figure out how many records we wrote to Cassandra and report it
    val cResultSet: com.datastax.driver.core.ResultSet = CassandraConnector(sc.getConf).withSessionDo { session =>
	  session.execute("select count(*) from pilotparse.wetpaths")
	}
	val countRow: com.datastax.driver.core.Row = cResultSet.one
	// Ok, this is super esoteric. So a cassandra type of int is really bigint which then 
    // looks in the return type as Columns[count(bigint)] but the corresponding Java type 
    // is a Long as per https://docs.datastax.com/en/developer/java-driver/3.1/manual/
    // 
	// So if you go count.getInt("count") or count.getVarint("count") the driver throws a 
	// conversion codec error. The proper getter method is getLong("count") ... irritating.
    // Wish I could get that 45 minutes back.
    print(s"Wrote ${countRow.getLong("count").toString} records to pilotparse.wetpaths")

    print("\n\n>>>>>> END OF PROGRAM <<<<<<\n\n");
  }
}
