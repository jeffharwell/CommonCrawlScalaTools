package com.jeffharwell.commoncrawl.createcorpus.commoncrawlimport

import com.datastax.spark.connector.types.CassandraOption
import com.datastax.spark.connector.writer._
import com.datastax.driver.core.ConsistencyLevel
import com.jeffharwell.commoncrawl.warcparser.{FourForumsWARCTopicFilter, Parser, ParserTooSlowException, WARCCategorizer, WARCRecord}
import org.apache.spark.SparkConf

import java.io.BufferedInputStream
import java.net.URL
import java.time.Instant
import scala.collection.mutable.ListBuffer

class ProcessWETPaths(commoncrawl_url: String, spark_conf: SparkConf) extends java.io.Serializable {
  // Stores the URL where we are accessing Common Crawl from AWS
  // Should be something like "https://data.commoncrawl.org/"
  val commoncrawl_aws_url: String = {
    if (commoncrawl_url.charAt(commoncrawl_url.length-1) != '/') {
      commoncrawl_url + "/"
    } else {
      commoncrawl_url
    }
  }
  val ignoreNullsWriteConf: WriteConf = WriteConf.fromSparkConf(spark_conf).copy(ignoreNulls = true,
    consistencyLevel = ConsistencyLevel.ONE)

  // Our Function to Write a WARC record to the correct table in Cassandra
  def writeToCassandra(wc: WARCRecord, category: Set[String], wet_url: String): WARCCassandraBindWithCategories = {
    val opt_url: scala.Option[String] = Some(wet_url) : scala.Option[String]

    WARCCassandraBindWithCategories(CassandraOption.unsetIfNone(wc.get("WARC-Record-ID")),
      category,
      CassandraOption.unsetIfNone(wc.getContent()),
      CassandraOption.unsetIfNone(wc.get("Content-Length")),
      CassandraOption.unsetIfNone(wc.get("Content-Type")),
      CassandraOption.unsetIfNone(None), // lucene dummy column
      CassandraOption.unsetIfNone(wc.get("WARC-Date")),
      CassandraOption.unsetIfNone(opt_url),
      CassandraOption.unsetIfNone(wc.get("WARC-Target-URI")),
      CassandraOption.unsetIfNone(wc.get("Top-Level-Domain")))
  }

  // Function to discard anything that doesn't have a category
  /*
   * This is not the right way to do this, it does not successfully discard records with no
   * categories
  def writeByCategory(wc: WARCRecord, wet_path: String): WARCCassandraBindWithCategories = {
    wc.getCategories() match {
      case Some(c) => writeToCassandra(wc, c, wet_path) //writeToCassandraByCategory(wc, c)
      case _ => writeToCassandra(wc, Set(), wet_path) // write it with an empty category set
    }
  }
   */

  // Define our function to parse a given WET archive path
  def parseWETArchiveURL[A <: WARCCategorizer](wet_path: String, cassandraConnectorConf: com.datastax.spark.connector.cql.CassandraConnectorConf, warcCategorizer: A, retries: Integer = 0): List[WARCCassandraBindWithCategories] = {
    val url = new URL(s"$commoncrawl_aws_url$wet_path")

    // Create the categorizer
    //val c: FourForumsWARCCategorizer = new FourForumsWARCCategorizer(1, 2)
    val c = warcCategorizer

    // Create the start and finish triggers
    val myStartTrigger = new MyStartTrigger(cassandraConnectorConf, wet_path)
    val myFinishTrigger = new MyFinishTrigger(cassandraConnectorConf, wet_path)

    // Parse the records and load the list buffer
    val records = ListBuffer[WARCRecord]()

    // Note when we started
    val start_seconds = Instant.now().getEpochSecond
    println(s">> Started Parsing $wet_path at ${Instant.now()}")

    /*
     * Add the rate limit. Basically the system at Fuller runs through the average WET Archive File in less than 6 minutes at the most.
     * This is about (6 min * 60 sec/min * 1000 ms/sec)/55000 records = 6.55 records/ms. So increase this by 50% to give us
     * some headroom: 9.82 records / ms. Expect the parser to keep that average rate over any 1000 documents.
     */
    val mav_avg_ms_per_record = 100
    val average_over = 500

    try {
      val urlconnection = url.openConnection()
      urlconnection.setReadTimeout(60000)
      urlconnection.setConnectTimeout(60000)
      println(s"   Opening connection to $commoncrawl_aws_url$wet_path")

      val parser_from_aws = Parser(new BufferedInputStream(urlconnection.getInputStream), c)
      parser_from_aws.addStartTrigger(myStartTrigger)
      parser_from_aws.addFinishTrigger(myFinishTrigger)
      // Set up the rate limit, see comments above.
      parser_from_aws.setRateLimit(mav_avg_ms_per_record, average_over)
      //parser_from_aws.setDebugRate() // if you want to see debug messages regarding the rate of parsing
      //parser_from_aws.setDebugMemory() // Turn on Memory usage messages from the parser

      // Do the parse with the filter
      //parser_from_aws.withFilter(myfilter(_)).foreach((wc: WARCRecord) => records += wc)
      parser_from_aws.foreach((wc: WARCRecord) => records += wc)


      // Note when we finished and log it
      println(s">> Finished Parsing $wet_path at ${Instant.now()}")
      val finish_seconds = Instant.now().getEpochSecond
      println(s"   $wet_path Elapsed time = ${finish_seconds - start_seconds} seconds")
      val rate = records.size.toDouble / (finish_seconds - start_seconds)
      println(s"   Filter extraction rate was $rate records / sec")

      Thread.sleep(30000) // don't hit AWS so hard, pause for 30 seconds before the next record
    } catch {
      case error: java.net.SocketException =>
        if (retries < 0) { // once we fail twice we never seem to succeed, so only try once or twice with a significant back off, specifically 5? minutes (1+2+3+4+5)
          val retry_seconds = 60000
          println(s">> Parsing $wet_path Failed at ${Instant.now()}")
          println(s"   $error")
          println(s"   Connection Error, the socket reset, sleeping for ${retry_seconds * (retries + 1)} ms before retrying")
          val finish_seconds = Instant.now().getEpochSecond
          println(s"   $wet_path Elapsed time = ${finish_seconds - start_seconds} seconds")
          Thread.sleep(retry_seconds * (retries + 1))
          parseWETArchiveURL(wet_path, cassandraConnectorConf, c, retries + 1)
        } else {
          val finish_seconds = Instant.now().getEpochSecond
          println(s"   Connection Error, the socket reset and we have failed $retries times, erroring out.")
          println(s"   $error")
          println(s"   We collected ${records.size} record(s) from $wet_path. We will write those records to the database.")
          println(s"   This WARC file will not be marked complete in the database and may be sampled again later.")
          println(s"   $wet_path Elapsed time = ${finish_seconds - start_seconds} seconds")
          println(s">> Parsing $wet_path Failed at ${Instant.now()}")
        }
      case error: java.net.UnknownHostException => // this is thrown if the new URL() command can't resolve the hostname
        if (retries < 15) { // we are much more likely to recover from this then the socket reset error, and if we fail at this point there are no partial records.
          // So try harder.
          val retry_seconds = 5000
          val finish_seconds = Instant.now().getEpochSecond
          println(s"   $error")
          println(s"   Could not open the URL, unknown host name (what the heck causes that!!), sleeping for ${retry_seconds * (retries + 1)} ms before retrying")
          println(s"   $wet_path Elapsed time = ${finish_seconds - start_seconds} seconds")
          println(s">> Parsing $wet_path Failed at ${Instant.now()}")
          Thread.sleep(retry_seconds * (retries + 1))
          parseWETArchiveURL(wet_path, cassandraConnectorConf, c, retries + 1)
        } else {
          val finish_seconds = Instant.now().getEpochSecond
          println(s"  Connection Error: failed $retries times to resolve $commoncrawl_aws_url$wet_path.")
          println(s"   $error")
          println(s"  We collected ${records.size} records.")
          println(s"  This WARC file will not be marked complete in the database and may be sampled again later.")
          println(s"  $wet_path Elapsed time = ${finish_seconds - start_seconds} seconds")
          println(s">> Parsing $wet_path Failed at ${Instant.now()}")
        }
      case error: ParserTooSlowException => // going too slowly
        if (retries < 1) { // we are probably not going to recover, but try one more time
          val retry_seconds = 60000
          println(s">> FAILED: Parsing $wet_path failed at ${Instant.now()}")
          println(s"   Error Message: $error")
          println(s"   Parser was running too slow, fell below $mav_avg_ms_per_record milliseconds per record. Sleeping for ${retry_seconds * (retries + 1)} ms before retrying")
          val finish_seconds = Instant.now().getEpochSecond
          println(s"   $wet_path Elapsed time = ${finish_seconds - start_seconds} seconds")
          Thread.sleep(retry_seconds * (retries + 1))
          parseWETArchiveURL(wet_path, cassandraConnectorConf, c, retries + 1)
        } else {
          val finish_seconds = Instant.now().getEpochSecond
          println(s"   Error Message: $error")
          println(s"   Parser running too slowly, fell below $mav_avg_ms_per_record milliseconds per record. We retried $retries times, erroring out.")
          println(s"   We collected ${records.size} record(s) from $wet_path. We will write those records to the database.")
          println(s"   This WARC file will not be marked complete in the database and may be sampled again later.")
          println(s"   $wet_path Elapsed time = ${finish_seconds - start_seconds} seconds")
          println(s">> FAILED: Parsing $wet_path Failed at ${Instant.now()}")
        }
    }
    //records.map(x => writeByCategory(x, wet_path)).toList

    // See https://stackoverflow.com/questions/42935572/best-way-to-unpack-an-option-field-inside-a-map-operation-in-scala
    // and https://alvinalexander.com/scala/best-practice-option-some-none-pattern-scala-idioms/
    // "The Option is a collection with zero or more elements, the foreach method can be used in many situations"
    // Note that r is a WARCConversion so .getCategories() returns Options[Set[String]]. This does not directly call the
    // getCategories() method of the WARCCategorizer that we passed to the parser.
    //
    // If Option[Set[String]] is None then the record will just not be yielded by the for comprehension.
    val lb: ListBuffer[WARCCassandraBindWithCategories] = for {
      r <- records
      c <- r.getCategories() // this does an implicit unwrap of the Some(), if it is None, then we don't yield, just continue with the next record r
    } yield writeToCassandra(r, c, wet_path)

    lb.toList
  }
}

