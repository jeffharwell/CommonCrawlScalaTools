#!/bin/bash

SCALAVERSION="2.11"
VERSION=`cat ./build.sbt | grep version | awk '{print $3}' | sed 's/"//g'`
echo ${VERSION}
NAME="createcorpus"
ORGANIZATION="com.jeffharwell"
K8SCRIPTBASE="~/nginx_jar_repo"
PODNAME="web-0"

K8USER="k8"
K8HOST="k8v17master.fuller.edu"

JARLOCATION="./target/scala-2.11/CreateCorpus-assembly-${VERSION}.jar"

md5sum ${JARLOCATION}
scp ${JARLOCATION} ${K8USER}@${K8HOST}:${K8SCRIPTBASE}
ssh ${K8USER}@${K8HOST} kubectl cp ${K8SCRIPTBASE}/CreateCorpus-assembly-${VERSION}.jar ${PODNAME}:/usr/share/nginx/html/
ssh ${K8USER}@${K8HOST} kubectl exec  ${PODNAME} -- md5sum /usr/share/nginx/html/CreateCorpus-assembly-${VERSION}.jar
