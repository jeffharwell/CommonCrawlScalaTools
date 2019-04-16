#!/bin/bash

SCALAVERSION="2.11"
VERSION=`cat ./build.sbt | grep version | awk '{print $3}' | sed 's/"//g'`
NAME="createcorpus"
ORGANIZATION="com.jeffharwell"
K8SCRIPTBASE="~/nginx_jar_repo"

K8USER="k8"
K8HOST="k8master.fuller.edu"

LOCALMAVENREPO=${HOME}/.m2/repository
MVNLOCAL="${HOME}/.m2/repository/com/jeffharwell/${NAME}_${SCALAVERSION}"

sbt package
## turn echo back on after the sbt command (it leaves it off annoyingly)
stty echo

mvn install:install-file -Dfile=./target/scala-${SCALAVERSION}/${NAME}_${SCALAVERSION}-${VERSION}.jar -DgroupId=${ORGANIZATION} -DartifactId=${NAME}_${SCALAVERSION} -Dversion=${VERSION} -Dpackaging=jar -DlocalRepositoryPath=${LOCALMAVENREPO} -DcreateChecksum=true

for i in "xml" "xml.sha1" "xml.md5"
do
    mv ${MVNLOCAL}/maven-metadata-local.${i} ${MVNLOCAL}/maven-metadata.${i}
done

#scp -r ${MVNLOCAL} k8@k8master.fuller.edu:${K8SCRIPTBASE}/${NAME}_${SCALAVERSION}-${VERSION}/m2

#ssh k8@k8master.fuller.edu "${K8SCRIPTBASE}/${NAME}_${SCALAVERSION}-${VERSION}/publish_files.sh"
#scp -r ${MVNLOCAL} k8@k8master.fuller.edu:${K8SCRIPTBASE}/${NAME}_${SCALAVERSION}/m2
rsync -arv -e ssh ${MVNLOCAL} k8@k8master.fuller.edu:${K8SCRIPTBASE}/${NAME}_${SCALAVERSION}/m2
ssh k8@k8master.fuller.edu "${K8SCRIPTBASE}/${NAME}_${SCALAVERSION}/publish_files.sh"
