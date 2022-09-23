package com.jeffharwell.commoncrawl.createcorpus.commoncrawlimport

import com.datastax.spark.connector.toSparkContextFunctions
import org.apache.spark.SparkContext

/*
This class is not serializable. It should be run from the executor to select the WETPaths that we want to then
have the workers process.
 */
class GetWETPaths(spark_context: SparkContext) {
  // Stores the URL where we are accessing Common Crawl from AWS
  // Should be something like "https://commoncrawl.s3.amazonaws.com/"
  var sc: SparkContext = spark_context

  def getIncompleteWETPaths: List[(String, Boolean, Boolean)] = {
    /*
     * Creates a vector of WET archive URLs that have been started but not finished
     */

    // Get the WET paths from Cassandra ... lazily
    val wetPaths = sc.cassandraTable("pilotparse", "wetpaths")

    // All the wet paths that are started but not finished
    val allpaths = wetPaths.map{x => (x.get[String]("wet_path"), x.get[Boolean]("finished"), x.get[Boolean]("started"))}
    val incomplete = allpaths.filter{x => !x._2 && x._3}
    val incomplete_list = incomplete.collect()
    val number_of_incomplete = incomplete_list.length
    println(s"Retrieved $number_of_incomplete WET URLs that are not completely parsed.")

    incomplete_list.toList
  }

  def selectTenRandomWETPaths(): List[String] = {
    val wet_paths = sc.cassandraTable("pilotparse", "random_wetpaths")
    val all_rdd = wet_paths.map { x => x.get[String]("wet_path")}
    val all = all_rdd.collect()
    all.toList.slice(0, 10)
  }

  def getRandomWETPaths(numberOfPaths: Integer): List[(String, Boolean)] = {
    /*
     * Creates a vector of ${numberOfPaths} random WET archive URLs for us to parse later
     */

    // Get the WET paths from Cassandra ... lazily
    val wetPaths = sc.cassandraTable("pilotparse", "wetpaths")

    // All the wet paths that still need to be processed
    val allunfinished = wetPaths.map { x => (x.get[String]("wet_path"), x.get[Boolean]("finished")) }.filter { x => x._2 == false }

    // Get the number of wet paths for our random selection later
    val numberOfWetPaths = allunfinished.count.toInt
    println(s"Retrieved $numberOfWetPaths unfinished WET paths")

    // Create a list of random indexes to use when selecting WET paths
    val random = scala.util.Random
    val randomWetIndexes = for (i <- 1 to numberOfPaths) yield random.nextInt(numberOfWetPaths - 1)
    println(s"Finished generating $numberOfPaths random indexes")
    println(s"Number of Random Wet Indexes: ${randomWetIndexes.length}")

    // Index our RRD (https://stackoverflow.com/questions/26828815/how-to-get-element-by-index-in-spark-rdd-java) which is a super neat trick
    // This data is small enough to be able to pull it right into memory as a List, but this is a bit more ... "spark"
    val wetPathsWithIndex = allunfinished.zipWithIndex
    val wetPathsIndexKey = wetPathsWithIndex.map { case (k, v) => (v, k) }
    println("Finished Indexing the unfinished wet paths")

    // Now cache this structure, otherwise it gets recomputed every time we do a lookup in the below map, which is not very efficient and super slow
    wetPathsIndexKey.cache
    println("Wet Paths have been cached")

    // Yes, the below code proved that the zipWithIndex method that was used above
    // or perhaps the map used to flip the keys and values, or perhaps the way
    // lookup works, ends up with missing indexes. Some will just return a
    // java.lang.ArrayIndexOutOfBoundsException. My personal guess is that the RDD on
    // certain workers just doesn't have the requested index. At any rate, since I just
    // care about a random sample anyway, we just need to work around this.
    /*
    var totalNumberOfKeys = wetPathsIndexKey.count().toInt
    println(s"We have ${totalNumberOfKeys} to check/retrieve")
    var allkeys = for (i <- 0 to (totalNumberOfKeys-1)) yield {
        if (i % 1000 == 0) {
            println(s"Finished Checking All Keys below ${i}")
        }
        var key = wetPathsIndexKey.lookup(i)(0)
    }
    println("Done getting all possible keys")
    */

    // Now grab the random wet paths using the list of random index values we computed earlier
    //
    // If working directly with a table
    // the extra (0) index is b/c the lookup method returns a set and, for this use case, it is always only 1 element long
    // so val randomWetPaths = randomWetIndexes.map(i => wetPathsIndexKey.lookup(i)(0).get[String]("wet_path"))

    // Lookup the path associated with this index. But if it doesn't return (as happens on occasion) then return the string
    // "NoPathFound"
    def getPathIfExists(index: Int): (String, Boolean) = {
      try {
        wetPathsIndexKey.lookup(index)(0)
      } catch {
        case e: java.lang.ArrayIndexOutOfBoundsException => {
          ("NoPathFound", false)
        }
      }
    }

    val randomWetPathsMap = randomWetIndexes.map(i => getPathIfExists(i))
    // filter out any "NoPathFound" strings which indicates that the random index did not return a path
    val randomWetPaths = randomWetPathsMap.filter(_._1 != "NoPathFound")

    println("Finished creating randomWetPaths RDD")
    // Return a list
    randomWetPaths.toList
  }
}

