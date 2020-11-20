# recommendation-system
Big Data Analytics project that'll focus on recommendation system

## This project will focus on UserUser collaborative filtering and Content-based recommendation system



## MySQL potential issues
Run
`show global variables like 'local_infile';`
within mysql if you're getting an issue like `Exception in thread "main" java.sql.SQLSyntaxErrorException: Loading local data is disabled; this must be enabled on both the client and server sides`

if `local_infile` is OFF, you'll need to turn it on by running `set global local_infile=true;`


## Python libraries
To run the actual models, you'll need to install few python libraries. You need `pip` to install the libraries.
* `pip3 install mysql-connector-python`
* `pip3 install mysql-connector-python-rf`
* `pip3 install lenskit`

## Data Preparation and loading
In order to prepare and merge the data, you'll need to run `DataLoading.java`
Due to time limit, we were not able to add parameters for the program, you'll need to manually add
the correct data file paths in order to run it.


You'll need to set the classpath when you're running the JAVA code to load the data to MySQL.
Example: `export CLASSPATH=~/Downloads/mysql-connector-java-8.0.18/mysql-connector-java-8.0.18.jar:.`


## UserUser Collaborative Filtering and Content-based recommendation system
In order to run the models, you'll simply need to run the appropriate python3 files
* `user_user_collaborative_filtering.py`
* `content_based_recommendation_system.py`
