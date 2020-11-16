# recommendation-system
Big Data Analytics project that'll focus on recommendation system

## This project will focus on UserUser collaborative filtering and content-based filtering

You'll need to set the classpath when you're running the JAVA code to load the data to MySQL.
Example: `export CLASSPATH=~/Downloads/mysql-connector-java-8.0.18/mysql-connector-java-8.0.18.jar:.`


`show global variables like 'local_infile';` run this within mysql if you're getting an issue like `Exception in thread "main" java.sql.SQLSyntaxErrorException: Loading local data is disabled; this must be enabled on both the client and server sides`

if `local_infile` is OFF, you'll need to turn it on by running `set global local_infile=true;`


To run the actual models, you'll need to install few python libraries. You need `pip` to install the libraries.
* `pip3 install mysql-connector-python`
* `pip3 install mysql-connector-python-rf`
* `pip3 install lenskit`