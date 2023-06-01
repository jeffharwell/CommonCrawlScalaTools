#!/bin/bash

export PATH="/usr/lib/jvm/java-8-openjdk/bin/:$PATH"

sbt clean
sbt test
stty echo
