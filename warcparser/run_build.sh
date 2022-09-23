#!/bin/bash

echo "Cleaning"
sbt clean
echo "Build and Test"
sbt test
stty echo
