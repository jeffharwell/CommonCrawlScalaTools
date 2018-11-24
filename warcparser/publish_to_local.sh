#!/bin/bash

echo "Don't use this. Instead use 'sbt publishLocal'"
echo "Sbt will publish the jar to the ~/.ivy2 directory where it can be called by other projects."
echo "The code in this bash script will publish a jar file to the local Maven repo ~/.m2."
echo "But that is unnecessary complexity!"
exit

SCALAVERSION="2.11"
VERSION=`cat ./build.sbt | grep version | awk '{print $3}' | sed 's/"//g'`
NAME="warcparser"
ORGANIZATION="com.jeffharwell"
LOCALMAVENREPO=${HOME}/.m2/repository

sbt package

mvn install:install-file -Dfile=./target/scala-${SCALAVERSION}/${NAME}_${SCALAVERSION}-${VERSION}.jar -DgroupId=${ORGANIZATION} -DartifactId=${NAME}_${SCALAVERSION} -Dversion=${VERSION} -Dpackaging=jar -DlocalRepositoryPath=${LOCALMAVENREPO} -DcreateChecksum=true

