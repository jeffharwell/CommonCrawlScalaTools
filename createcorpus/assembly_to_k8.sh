#!/bin/bash

VERSION=`cat ./build.sbt | grep version | awk '{print $3}' | sed 's/"//g'`
JAR="CreateCorpus-assembly-${VERSION}.jar"
LOCALJAR="./target/scala-2.10/${JAR}"
K8SCRIPTBASE="~/nginx_jar_repo"

K8USER="k8"
K8HOST="k8master.fuller.edu"
K8="${K8USER}@${K8HOST}"

#sbt assembly

#scp -r ${MVNLOCAL} k8@k8master.fuller.edu:${K8SCRIPTBASE}/${NAME}_${SCALAVERSION}-${VERSION}/m2
scp ${LOCALJAR} ${K8}:${K8SCRIPTBASE}/lib

#ssh k8@k8master.fuller.edu "${K8SCRIPTBASE}/${NAME}_${SCALAVERSION}-${VERSION}/publish_files.sh"
#scp -r ${MVNLOCAL} k8@k8master.fuller.edu:${K8SCRIPTBASE}/${NAME}_${SCALAVERSION}/m2
#rsync -arv -e ssh ${MVNLOCAL} k8@k8master.fuller.edu:${K8SCRIPTBASE}/${NAME}_${SCALAVERSION}/m2

ssh ${K8} "${K8SCRIPTBASE}/publish_jar.sh ${JAR}"
