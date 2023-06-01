#!/bin/bash

export PATH="/usr/lib/jvm/java-8-openjdk/bin/:$PATH"

echo "Cleaning"
sbt clean
echo "Build and Test"
sbt test
stty echo
