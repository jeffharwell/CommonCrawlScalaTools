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

SPARK_HOME=${HOME}/bin/spark/spark-1.6.3-bin-hadoop2.6/
SPARK_CLUSTER="spark://k8master.fuller.edu:7077"
CLASS_TO_CALL="com.jeffharwell.commoncrawl.createcorpus.hello"
VERSION=`cat ./build.sbt | grep ^version | awk '{print $3}' | sed 's/"//g'`

$SPARK_HOME/bin/spark-submit --class ${CLASS_TO_CALL} --master ${SPARK_CLUSTER} ./target/scala-2.10/CreateCorpus-assembly-${VERSION}.jar
