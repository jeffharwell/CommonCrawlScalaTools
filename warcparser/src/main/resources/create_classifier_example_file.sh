#!/bin/bash

wget https://commoncrawl.s3.amazonaws.com/crawl-data/CC-MAIN-2016-50/segments/1480698540409.8/wet/CC-MAIN-20161202170900-00007-ip-10-31-129-80.ec2.internal.warc.wet.gz
gunzip CC-MAIN-20161202170900-00007-ip-10-31-129-80.ec2.internal.warc.wet.gz
dd if=./CC-MAIN-20161202170900-00007-ip-10-31-129-80.ec2.internal.warc.wet of=./classifier_example_file.wet count=50000
gzip ./classifier_example_file.wet
rm ./CC-MAIN-20161202170900-00007-ip-10-31-129-80.ec2.internal.warc.wet
