package com.jeffharwell.commoncrawl.warcparser

import com.datastax.spark.connector.cql.{CassandraConnector, CassandraConnectorConf}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

/*
 * Stats writer implementation that can receive statistics from a Stream Filter and write them to a Cassandra
 * datastore in the Spark context.
 *
 * Cassandra Schema:
 *
 CREATE TABLE pilotparse.topic_filter_stats (
    warc_record_id text,
    measurement_type text,
    measurement bigint,
    PRIMARY KEY (warc_record_id, measurement_type)
) WITH compaction = {'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy'}
 *
 *
 */
class TopicFilterStatsWriter(cassandra_conf: CassandraConnectorConf) extends StatsWriter {
  val cc = new CassandraConnector(cassandra_conf)

  /*
   * This is a Scala Future, it will write the statistics to the datastore using a different thread so not to block
   * the main filter execution thread.
   */
  def write_arguments(id: String, measurement_type: String, measurement: Long): Future[Unit] = Future {
    cc.withSessionDo { session =>
      session.execute(s"insert into pilotparse.topic_filter_stats (warc_record_id, measurement_type, measurement) values ('${id}', '${measurement_type}', ${measurement})")
    }
  }

  /*
   * The apply function. It gets the ID of the WARC record in question and then calls the future
   * to write the stats to the datastore asynchronously.
   */
  def apply[A <: WARCRecord](warc_record: A, measurement_type: String, measurement: Long): Unit = {
    val id: String = warc_record.get("WARC-Record-ID") match {
      case Some(s: String) => s
      case _ => "No WARC ID Returned"
    }

    write_arguments(id, measurement_type, measurement)
  }

  def apply[A <: WARCRecord](warc_record: A, description: String, flag_one: Long, flag_two: Long, flag_three: Long, processing_time: Long): Unit = {
    // This is isn't really applicable to the Topic Filter stats
    // Write the flags and the processing time
    val id: String = warc_record.get("WARC-Record-ID") match {
      case Some(s: String) => s
      case _ => "No WARC ID Returned"
    }

    write_arguments(id, s"$description $flag_one $flag_two $flag_three", processing_time)
  }

}