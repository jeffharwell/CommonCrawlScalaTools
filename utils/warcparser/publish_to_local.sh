#!/bin/bash

SCALAVERSION="2.10"
VERSION=`cat ./build.sbt | grep version | awk '{print $3}' | sed 's/"//g'`
NAME="warcparser"
ORGANIZATION="com.jeffharwell"

sbt package

mvn install:install-file -Dfile=./target/scala-${SCALAVERSION}/${NAME}_${SCALAVERSION}-${VERSION}.jar -DgroupId=${ORGANIZATION} -DartifactId=${NAME}_${SCALAVERSION} -Dversion=${VERSION} -Dpackaging=jar -DlocalRepositoryPath=/home/jharwell/.m2/repository -DcreateChecksum=true

