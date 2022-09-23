package com.jeffharwell.commoncrawl.warcparser

import com.datastax.spark.connector.cql.{CassandraConnector, CassandraConnectorConf}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

/*
 * Stats writer implementation that can receive statistics from a Stream Filter and write them to a Cassandra
 * datastore in the Spark context.
 */
class StreamFilterStatsWriter(cassandra_conf: CassandraConnectorConf) extends StatsWriter {
  val cc = new CassandraConnector(cassandra_conf)

  /*
   * This is a Scala Future, it will write the statistics to the datastore using a different thread so not to block
   * the main filter execution thread.
   */
  def write_arguments(id: String, measurement_type: String, measurement: Long): Future[Unit] = Future {
    cc.withSessionDo { session =>
      session.execute(s"insert into pilotparse.stream_filter_stats (warc_record_id, measurement_type, measurement) values ('${id}', '${measurement_type}', ${measurement})")
    }
  }

  /*
  CREATE TABLE pilotparse.stream_filter_stats_compact (
    warc_record_id text,
    description text,
    value_one bigint,
    value_two bigint,
    value_three bigint,
    processing_time bigint,
    PRIMARY KEY (warc_record_id, description)
   )
   */
  def write_compact_arguments(id: String, description: String, value_one: Long, value_two: Long, value_three: Long,
                              processing_time: Long): Future[Unit] = Future {
    cc.withSessionDo { session =>
      val sql =
        s"""
          |insert into pilotparse.stream_filter_stats_compact (warc_record_id, description, value_one, value_two, value_three, processing_time)
          |values ('${id}', '${description}', ${value_one}, ${value_two}, ${value_three}, ${processing_time})
          |""".stripMargin
      session.execute(sql)
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

  def apply[A <: WARCRecord](warc_record: A, description: String, accepted: Long, rejected_detail: Long,
                             rejected_no_detail: Long, processing_time: Long): Unit = {
    val id: String = warc_record.get("WARC-Record-ID") match {
      case Some(s: String) => s
      case _ => "No WARC ID Returned"
    }
    write_compact_arguments(id, description, accepted, rejected_detail, rejected_no_detail, processing_time)
  }
}