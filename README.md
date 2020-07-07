# Data Lake

This is an example data lake application using Spark, Python, AWS EMR and S3. The app demonstrates basic big data patterns related to schema design, and data loading, transformation, and partitioning.

The data reflects usage of an imaginary music streaming service called Sparkify. There are records for songs (from the Million Song Dataset) and songplays (produced by an event generator). Both are stored in JSON format in S3 buckets. The Spark application builds a batch job, reads in data from the buckets, and then saves the transformed data back into S3 buckets as partitioned Parquet files.

This code can run locally, but is expected to be deployed in an AWS EMR instance.

## Files
* `dl.cfg` configuration values necessary for communicating with AWS: user public and secret keys
* `etl.py` copies data from S3 into Spark job, then transforms and saves data as fact/dimension tables 

## process
local
* edit `dl.cfg` to include necessary values
* run `python etl.py`
aws
* edit `dl.cfg` to include necessary values
* scp `dl.cfg` and `etl.py` into EMR instance
* ssh into EMR instance
* run `submit-spark --master yarn ./etl.py`