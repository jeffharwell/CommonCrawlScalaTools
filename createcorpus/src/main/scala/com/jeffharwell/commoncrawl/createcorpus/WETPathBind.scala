package com.jeffharwell.commoncrawl.createcorpus

import java.sql.Timestamp

// The class and function needed to parse the WET Path lines in the file 
// and map them to the schema of the Cassandra table
case class WETPathBind(wet_path:Option[String], file_name:Option[String],
                        started_processing:Option[Timestamp], finished_processing:Option[Timestamp], successfully_processed:Option[Boolean],
                        records_extracted:Option[Int], log_message:Option[String])

