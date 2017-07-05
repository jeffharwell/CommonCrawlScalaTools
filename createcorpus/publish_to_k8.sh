#!/bin/bash

SCALAVERSION="2.10"
VERSION="0.0.1"
NAME="createcorpus"
ORGANIZATION="com.jeffharwell"
K8SCRIPTBASE="~/nginx_jar_repo"

K8USER="k8"
K8HOST="k8master.fuller.edu"

MVNLOCAL="/home/jharwell/.m2/com/jeffharwell/${NAME}_${SCALAVERSION}"

sbt package

mvn install:install-file -Dfile=./target/scala-${SCALAVERSION}/${NAME}_${SCALAVERSION}-${VERSION}.jar -DgroupId=${ORGANIZATION} -DartifactId=${NAME}_${SCALAVERSION} -Dversion=${VERSION} -Dpackaging=jar -DlocalRepositoryPath=/home/jharwell/.m2 -DcreateChecksum=true

for i in "xml" "xml.sha1" "xml.md5"
do
    mv ${MVNLOCAL}/maven-metadata-local.${i} ${MVNLOCAL}/maven-metadata.${i}
done

scp -r ${MVNLOCAL} k8@k8master.fuller.edu:${K8SCRIPTBASE}/${NAME}_${SCALAVERSION}-${VERSION}/m2

ssh k8@k8master.fuller.edu "${K8SCRIPTBASE}/${NAME}_${SCALAVERSION}-${VERSION}/publish_files.sh"
