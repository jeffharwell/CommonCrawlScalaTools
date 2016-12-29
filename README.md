# CommonCrawlScalaTools

This is my collection of tools to manipulate Common Crawl data using Scala.

### wetpath_loader ###

This is my tool for reading the list of WET paths published by Common Crawl and loading them into a database table. To run:

clone the repo

> git clone https://github.com/jeffharwell/CommonCrawlScalaTools.git

Create a database with a table wetpaths that follows the schema in the database_schema.sql file in the root of this project. This file has the commands to create a database, create a user, and create the table, and grant permissions in a Mysql database.

Go to the wetpath_loader sbt project

> cd ./wetpath_loader

Copy the application.conf.example file to application.conf and edit the file to add the username, password and hostname of the computer hosting your database. You can also change the driver and url if you opt not to use MySql.

You will also need to provide the path of the file holding the list of WET Paths for the crawl you are working with. Common Crawl will provide this file for every crawl that they do. For example for the [December 2016 Crawl](http://commoncrawl.org/2016/12/december-2016-crawl-archive-now-available/) the WET file list is CC-MAIN-2016-50/wet.paths.gz. If you wanted to use it download it, unzip it, and drop it in the ./src/main/resources directory, then add the name of the file to application.conf as the value for general.wetpathfile.

> cp ./src/main/resources/application.conf.example ./src/main/resources/application.conf
> vi ./src/main/resources/application.conf

Now fire up sbt and run the loader

> $ sbt
> > run 

This will load the paths from the wet.paths file into the database table.
