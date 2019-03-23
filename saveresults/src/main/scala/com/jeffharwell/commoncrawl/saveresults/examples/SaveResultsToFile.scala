package com.jeffharwell.commoncrawl.saveresults.examples

/* Common Crawl Classes */
import com.datastax.driver.core.Cluster
import com.datastax.driver.core.Session
import com.datastax.driver.core.ResultSet
import com.datastax.driver.core.Row

/* Other Classes */
import java.util.Iterator
import scala.collection.mutable.ListBuffer
import java.nio.file.{Paths, Path, Files}
import java.nio.charset.Charset
import java.io.BufferedWriter
//import java.io.BufferedInputStream
//import java.io.FileInputStream
//import java.io.File
//import java.net.URL

/*
 * There are no real basic examples of using Scala with Cassandra, most are concerned
 * with wrapping the Java libraries so that they act more like Scala ... which gets deep and strange
 * pretty quickly with a lot of emphasis on abtractions like futures and promises.
 *
 * This code just needs to do a basic fetch and save to disk, it is really close to being a batch script.
 * So I basically just rewrote the Java quick start from the documentation using Scala. See:
 * https://docs.datastax.com/en/developer/java-driver/3.5/manual/
 */

object SaveResultsToFile {
  /*
   * Takes a warc_record_id string and retuns the filename
   */
  def getFilename(warc_record_id: String): String = {
    // Starts like <urn:uuid:409caca2-adf6-41f6-9bef-7c7c1cde3aab>
    val id_array = warc_record_id.split(':')

    // Returns something like 409caca2-adf6-41f6-9bef-7c7c1cde3aab.txt
    id_array(2).replace(">","")+".txt"
  }

  /*
   * Does a very, very basic check on the command line arguments that were passed
   */
  def checkUsage(args: Array[String]): Unit = {
    var msg = """
Usage: com.jeffharwell.commoncrawl.saveresults.examples.SaveResultsToFile hostname table directory

Where hostname is the hostname of the Cassandra cluster
Table is the table name in the Cassandra cluster from which to pull the records
Directory is the local directory name in which to write the results, it must already exist
    """

    // Make sure we at least were passed the correct number of arguments
    if (args.size != 3) {
      println("Invalid number of arguments.\n")
      println(msg)
      System.exit(1)
    }

    // Make sure the path exists
    if (!Files.exists(Paths.get(args(2)))) {
      println("The path "+args(2)+" does not exist please create it and try again\n")
      println(msg)
      System.exit(1)
    }
  }

  /*
   * Writes the file
   */
  def writeToFile(path: String, filename: String, lucene: String, content: String): Unit = {
    var fullpath: Path = Paths.get(path + "/" + filename)
    var writer: BufferedWriter = Files.newBufferedWriter(fullpath, Charset.forName("UTF-8"))
    writer.write(lucene+"\n\n")
    writer.write(content)
    writer.close()
  }
    
  /*
   * The Main Function, this connects to Cassandra, pulls down the records
   * in the specified table, and writes them to the specified location on the
   * file system.
   *
   * Is expecting: argument 1 to be the hostname of the Cassandra cluster
   *               argument 2 to be the table name in Cassandra
   *               argument 3 to be the local directory in which to save the files
   */
  def main(args: Array[String]): Unit = {

    // Make sure we got sane command line arguments
    checkUsage(args)
    var hostname = args(0)
    var table = args(1)
    var directory = args(2)

    var cluster:Cluster = Cluster.builder()
                            .addContactPoint(hostname)
                            .build()
    var session:Session = cluster.connect()
    var rs:ResultSet = session.execute("select release_version from system.local")
    var row:Row = rs.one()
    println(row.getString("release_version"))

    rs = session.execute("select * from "+table)
    var iter:java.util.Iterator[Row] = rs.iterator()

    // From https://docs.datastax.com/en/drivers/java/3.5/com/datastax/driver/core/PagingIterable.html#all--
    // It is the example usage of the fetchMoreResults method.
    while (iter.hasNext) {
      row = iter.next()
      var filename = getFilename(row.getString("warc_record_id"))
      writeToFile(directory, filename, row.getString("lucene"), row.getString("content"))
      println("Writing: "+filename+" "+row.getString("warc_record_id"))
    }
    session.close()
    cluster.close()
  }
}

