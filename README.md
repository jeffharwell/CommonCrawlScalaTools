# CommonCrawlScalaTools

This is my collection of tools to manipulate Common Crawl data using Scala.

### The Sample WARC Parser code

To run the example code

First get the repo and compile the code

    $ git clone https://github.com/jeffharwell/CommonCrawlScalaTools.git
    $ cd ./CommonCrawlScalaTools/warcparser
    $ sbt clean compile test

    $ sbt
    > runMain com.jeffharwell.commoncrawl.warcparser.examples.CountLocalFiltered

or 

    cd ./warcparser
    sbt "runMain com.jeffharwell.commoncrawl.warcparser.examples.CountLocalFiltered"

### Submitting The Hello App to Spark

This is a bit of a placeholder. The createcorpus library is set up to create an application
that can be packaged into a "fat jar" by sbt-assembly and then submitted directly to a Spark
cluster. The library has a dependency on the warcparser so you must first compile and publish
that code locally before you can compile and submit the createcorpus code.

Here are the instructions on how to run the full process from compile to submission. Note: you
will want to modify the ./createcorpus/sample_spark_submit.sh script so that it points to your Spark cluster.

First you need to make sure that the warcparser code has been published locally

    $ cd ./warcparser
    $ sbt clean compile publishLocal

The above command should have published the .jar file containing the warcparser libray to your local
.ivy2 directory. Now compile and assemble the createcorpus libary. The 'assembly' sbt task will create a 
jar file that also includes the warcparser code that you just compiled and published. This is important because
you can't depend on the remote Spark instance to have the warcparser library available so you must include it
in your submission.

    $ cd ../createcorpus
    $ sbt clean compile assembly

The code includes a sample script that uses the spark-submit program to submit the jar you just assembled to a 
Spark cluster. It assumes that you can the Spark binary installed in the following path: ${HOME}/bin/spark/spark-1.6.3-bin-hadoop2.6/. You will need to modify this script and tell it the URL of your spark cluster.

    $ sh ./sample_spark_submit.sh
