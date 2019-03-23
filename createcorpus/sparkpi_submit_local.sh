#!/bin/bash

##
# Sample command to submit the "fat jar" to Spark
# Set the SPARK_HOME variable to the location of your local
# spark code. 
# Set the SPARK_CLUSTER variable to the URL of the Spark Cluster
# that you want to run the jar file on.
#
# Note that CreateCorpus-assembly-${VERSION}.jar file is a "fat jar" created
#  by running 'sbt assembly'

#SPARK_HOME=${HOME}/bin/spark/spark-1.6.3-bin-hadoop2.6/
SPARK_HOME=${HOME}/bin/spark/spark-2.3.2-bin-hadoop2.7/
#SPARK_CLUSTER="spark://k8master.fuller.edu:7077"
SPARK_CLUSTER="spark://spark-master:7077"
#CLASS_TO_CALL="com.jeffharwell.commoncrawl.createcorpus.loadwetpaths"
VERSION=`cat ./build.sbt | grep ^version | awk '{print $3}' | sed 's/"//g'`

##$SPARK_HOME/bin/spark-submit --verbose --deploy-mode cluster --class ${CLASS_TO_CALL} --master ${SPARK_CLUSTER} ./target/scala-2.10/CreateCorpus-assembly-${VERSION}.jar
#$SPARK_HOME/bin/spark-submit --verbose --deploy-mode cluster --class ${CLASS_TO_CALL} --master ${SPARK_CLUSTER} http://nginx/lib/CreateCorpus-assembly-${VERSION}.jar
##$SPARK_HOME/bin/spark-submit --verbose --deploy-mode cluster --class ${CLASS_TO_CALL} --master ${SPARK_CLUSTER} --driver-class-path http://nginx/lib/CreateCorpus-assembly-${VERSION}.jar http://nginx/lib/CreateCorpus-assembly-${VERSION}.jar
$SPARK_HOME/bin/spark-submit \
  --class org.apache.spark.examples.SparkPi \
  --master ${SPARK_CLUSTER} \
  --deploy-mode cluster \
  --supervise \
  --executor-memory 2G \
  --total-executor-cores 4 \
  /opt/spark/examples/jars/spark-examples_2.11-2.3.2.jar \
  10000

#  --driver-class-path /opt/spark/lib/spark-assembly-2.3.2-hadoop2.7.0.jar \

##--jars http://nginx/lib/spark-assembly-1.6.3-hadoop2.6.0.jar \
