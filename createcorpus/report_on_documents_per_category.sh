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

SPARK_HOME=${HOME}/bin/spark/spark-2.3.2-bin-hadoop2.7/
SPARK_CLUSTER="spark://k8v17master.fuller.edu:6066"
CLASS_TO_CALL="com.jeffharwell.commoncrawl.createcorpus.commoncrawlimport.reportOnDocumentsParsedByCategory"
VERSION=`cat ./build.sbt | grep ^version | awk '{print $3}' | sed 's/"//g'`

$SPARK_HOME/bin/spark-submit \
    --verbose --deploy-mode cluster \
    --class ${CLASS_TO_CALL} \
    --master ${SPARK_CLUSTER} \
    --executor-memory 2G \
    --total-executor-cores 5\
    http://nginx/CreateCorpus-assembly-${VERSION}.jar
