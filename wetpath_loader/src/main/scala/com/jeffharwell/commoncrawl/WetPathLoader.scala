package com.jeffharwell.commoncrawl

import java.io.File
import java.sql.DriverManager
import java.sql.Connection
import scala.io.BufferedSource

class WetPathLoader(driver: String, url: String, username: String, password: String) {

  Class.forName(driver)
  val connection = DriverManager.getConnection(url, username, password)
  val statement = connection.createStatement()

  /*
  val resultSet = statement.executeQuery("select * from wetpaths");
  while (resultSet.next() ) {
    val id = resultSet.getInt("id")
    println(id)
  }
  */

  def loadPaths(wetfile: BufferedSource) = {
    var counter = 0
    var batchsize = 1000

    // Prepare the sql statement
    val sql = "insert into wetpaths (path) values (?)"
    val ps = connection.prepareStatement(sql)

    print("Working:")
    wetfile.getLines().foreach { l =>
      counter = counter + 1
      ps.setString(1, l)
      ps.addBatch()
      if (counter % batchsize == 0) {
        print('.')
        ps.executeBatch()
      }
    }

    // Catch any stragglers
    if ( counter % batchsize != 0) {
      ps.executeBatch()
    }

    ps.close()
    connection.close()
    println("\nDone");
  }
}
