package com.jeffharwell.commoncrawl.warcparser

import com.datastax.spark.connector.cql.{CassandraConnector, CassandraConnectorConf}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

/*
 * Stats writer implementation that can receive statistics from a Stream Filter and write them to a Cassandra
 * datastore in the Spark context.
 */
trait StatsWriter extends java.io.Serializable {

  def apply[A <: WARCRecord](w: A, measurement_type: String, measurement: Long): Unit

  def apply[A <: WARCRecord](warc_record: A, description: String, accepted: Long, rejected_detail: Long, rejected_no_detail: Long, processing_time: Long): Unit

}