## Project: Data Lake

### Overview

This project contains a script and configuration file to create a set of Spark 
DataFrames for the purposes of data analysis. The source of the tables are json files 
located in s3. Once the tables have been populated, they are persisted to partitioned
parquet files in s3.

The background for the project is described [here](https://github.com/george-zip/postgres_data_modeling#readme).

### Details

Project files:

[etl.py](etl.py) is a [pyspark](https://spark.apache.org/docs/latest/api/python/index.html) 
script that copies data from json files in S3 into staging tables, then loads it 
into the final destination tables.

[dl.cfg](dl.cfg) is a configuration file that contains environment-specific settings.

### How to run

#### Prerequisites 

The AWS password and secret must be defined in your environment before running 
`etl.py`:

```commandline
export KEY=<key>
export SECRET=<secret>
```

#### Performance

This script has been tested on the locally-installed Spark instance on my laptop and
on a three-node EMR cluster. A single run takes over 50 minutes on the EMR cluster. The 
performance seems to be related to the IO with S3. The reads require pulling many 
json files from nested subdirectories and the writes use S3 as well.

#### Run mode

An **optional** mode parameter may be passed to `etl.py` which allows the script to read
from local storage or HDFS and changes the s3 write mode from overwrite to ignore.
This was added to facilitate local testing. 

```commandline
spark-submit etl.py <mode>
```


