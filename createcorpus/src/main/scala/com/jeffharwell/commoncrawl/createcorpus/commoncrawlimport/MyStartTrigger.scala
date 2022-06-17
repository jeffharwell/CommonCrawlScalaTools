package com.jeffharwell.commoncrawl.createcorpus.commoncrawlimport

import com.datastax.spark.connector.cql.CassandraConnectorConf
import com.datastax.spark.connector.cql.CassandraConnector

/*
This class is used by the parseWETArchiveURL function. When we start processing a new URL we want to mark that
processing has begun in the wetpaths table.
 */

class MyStartTrigger(cassandraconf: CassandraConnectorConf, p_wet_path: String) extends com.jeffharwell.commoncrawl.warcparser.ParserTrigger {
  var conf: CassandraConnectorConf = cassandraconf
  var wet_path: String = p_wet_path

  override def call(extractedfilename: Option[String], parserrecordcount: Integer, parserlogmessage: Option[String]): Unit = {
    val cc = new CassandraConnector(conf)
    cc.withSessionDo { session =>
      session.execute(s"update pilotparse.wetpaths set started_processing = toTimestamp(now()), started = True where wet_path = '${wet_path}'")
    }
  }
}