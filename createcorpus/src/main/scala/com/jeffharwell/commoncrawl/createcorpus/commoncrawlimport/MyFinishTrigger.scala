package com.jeffharwell.commoncrawl.createcorpus.commoncrawlimport
/*
This class is used by the parseWETArchiveURL function. When we finish processing a new URL we want to mark that
processing was complete and record any error messages.
*/

import com.datastax.spark.connector.cql.CassandraConnectorConf
import com.datastax.spark.connector.cql.CassandraConnector

class MyFinishTrigger(cassandraconf: CassandraConnectorConf, p_wet_path: String) extends com.jeffharwell.commoncrawl.warcparser.ParserTrigger {
  var conf: CassandraConnectorConf = cassandraconf
  var wet_path: String = p_wet_path

  override def call(extractedfilename: Option[String], parserrecordcount: Integer, parserlogmessage: Option[String]): Unit = {
    // If we don't get a log message we want to leave it null
    val query = parserlogmessage match {
      case Some(msg) => s"""update pilotparse.wetpaths set
                        finished_processing = toTimestamp(now()),
                        log_message = '${msg}',
                        records_extracted = ${parserrecordcount},
                        finished = True
                      where wet_path = '${wet_path}'"""
      case None => s"""update pilotparse.wetpaths set
                        finished_processing = toTimestamp(now()),
                        records_extracted = ${parserrecordcount},
                        finished = True
                      where wet_path = '${wet_path}'"""
    }
    val cc = new CassandraConnector(conf)
    cc.withSessionDo { session =>
      session.execute(query)
    }
  }
}