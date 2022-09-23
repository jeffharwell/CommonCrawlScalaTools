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
CLASS_TO_CALL="com.jeffharwell.commoncrawl.createcorpus.commoncrawlimport.evaluateTopicFilterRandomWETPaths"
VERSION=`cat ./build.sbt | grep ^version | awk '{print $3}' | sed 's/"//g'`

###
#
# spark-submit will, by default, use the local path for the spark-assembly-1.6.3-hadoop2.6.0.jar, which
# is not correct on the Spark cluster and will cause the job to fail with the error like the following
#
# Launch Command: "/usr/lib/jvm/java-8-openjdk-amd64/bin/java" "-cp" "/opt/spark/conf/:/home/jharwell/bin/spark/spark-1.6.3-bin-hadoop2.6//lib/spark-assembly-1.6.3-hadoop2.6.0.jar:/opt/spark/lib/datanucleus-core-3.2.10.jar:/opt/spark/lib/datanucleus-api-jdo-3.2.6.jar:/opt/spark/lib/datanucleus-rdbms-3.2.9.jar" "-Xms1024M" "-Xmx1024M" "-Dakka.loglevel=WARNING" "-Dspark.master=spark://k8master.fuller.edu:7077" "-Dspark.driver.supervise=false" "-Dspark.app.name=com.jeffharwell.commoncrawl.createcorpus.loadwetpaths" "-Dspark.submit.deployMode=cluster" "-Dspark.jars=http://nginx/lib/CreateCorpus-assembly-0.0.1.jar" "-Dspark.rpc.askTimeout=10" "org.apache.spark.deploy.worker.DriverWrapper" "spark://Worker@10.244.3.43:33412" "/opt/spark/work/driver-20180804153953-0009/CreateCorpus-assembly-0.0.1.jar" "com.jeffharwell.commoncrawl.createcorpus.loadwetpaths"
# ========================================
# 
# Error: Could not find or load main class org.apache.spark.deploy.worker.DriverWrapper
#
# You can see in the initial java command the local path to the jar. Putting the cluster path of that jar
# as the argument to --driver-class-path solves this problem. I'm not sure why spark-submit thinks this is 
# the ONE jar that won't be in /opt/spark/lib ... but it does.
#
###

###
#
# If you go to spark web UI the output of the program can be found by clicking on the "Worker" link under
# the "Completed Drivers" section then scrolling down to the "Finished Drivers" section and clicking the
# "stdout" link in the "Logs" column in the row that matches the driver.
#
##

#
# See: https://spark.apache.org/docs/latest/running-on-kubernetes.html#client-mode to understand
# the headless service and the one member spark-worker-driver app
#

$SPARK_HOME/bin/spark-submit \
    --verbose --deploy-mode cluster \
    --class ${CLASS_TO_CALL} \
    --master ${SPARK_CLUSTER} \
    --executor-memory 2G \
    --total-executor-cores 3\
    http://nginx/CreateCorpus-assembly-${VERSION}.jar
