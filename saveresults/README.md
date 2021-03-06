## Introduction

Nothing fancy here. This is just a little program that connects to the Cassandra cluster and 
downloads the data collected for the gold standard. It is designed to work with a table that
looks like the following:

    CREATE TABLE pilotparse.golden_politics (
        warc_record_id text PRIMARY KEY,
        relevance text,
        content text,
        content_length text,
        content_type text,
        warc_target_uri text,
        warc_tldn text,
        wet_path text
    );

The contents of this table should be pretty small, on the order of 100 records, so the program
makes no attempts to limit RAM usage.

The program will pull each record from the cluster, and write it to a file with a filename that
is derived from the warc_record_id, so a name of <urn:uuid:6281751c-ddf5-4246-a60f-cd8a88f6cb30> 
would go to 6281751c-ddf5-4246-a60f-cd8a88f6cb30.txt.

The first line of the file will be the Lucene relevance score which tells you how relevant Lucene
thought the document was to the query. This will be followed by the document itself.

## Running the Program

You can run from within sbt as follows:

    $ sbt
    sbt:SaveResults> compile
    sbt:SaveResults> runMain com.jeffharwell.commoncrawl.saveresults.examples.SaveResultsToFile cassandra_host_name table_name path

Or in one shot without going to the interactive shell:

    $ sbt "runMain com.jeffharwell.commoncrawl.saveresults.examples.SaveResultsToFile cassandra table_name path"

So for my usual cluster, saving to the usual local directory, it looks like the following:

    sbt "runMain com.jeffharwell.commoncrawl.saveresults.examples.SaveResultsToFile k8master.fuller.edu pilotparse.golden_politics /home/jharwell/Documents/phd/golden_standard/politics_v20190421"
