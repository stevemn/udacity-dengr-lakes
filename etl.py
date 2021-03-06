import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import TimestampType
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .config("spark.hadoop.fs.s3a.access.key", os.environ['AWS_ACCESS_KEY_ID']) \
        .config("spark.hadoop.fs.s3a.secret.key", os.environ['AWS_SECRET_ACCESS_KEY']) \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """ Reads in Sparkify song data, transforms,and saves to parquet files.
        Song data is converted artist and song data.

    Arguments:
        spark {object}: a configured SparkSession object
        input_data {str}: path to input data for reading
        output_data {str}: path to write output data

    Returns:
        None
    """
    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select('song_id','title', 'artist_id', 'year',
                            'duration').distinct()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy(
        'year','artist_id').parquet(output_data + 'songs', mode='overwrite')

    # extract columns to create artists table
    artists_table = df.select('artist_id', df.artist_name.alias('name'),
                              df.artist_location.alias('location'),
                              df.artist_latitude.alias('latitude'),
                              df.artist_longitude.alias('longitude')
                             ).distinct()
    
    # write artists table to parquet files
    artists_table.write.parquet(output_data + 'artists', mode='overwrite')


def process_log_data(spark, input_data, output_data):
    """ Reads in Sparkify log data, transforms,and saves to parquet files.
        Log data is converted to user, time, and songplay data.

    Arguments:
        spark {object}: a configured SparkSession object
        input_data {str}: path to input data for reading
        output_data {str}: path to write output data

    Returns:
        None
    """
    # get filepath to log data file
    log_data = input_data + 'log_data/*/*/*.json'

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page=='NextSong')

    # extract columns for users table
    users_table = df.where(df.userId.isNotNull()).select(
                            df.userId.alias('user_id'),
                            df.firstName.alias('first_name'),
                            df.lastName.alias('last_name'),
                            'gender','level').distinct()
    
    # write users table to parquet files
    users_table.write.parquet(output_data + 'users', mode='overwrite')

    # create timestamp column from original timestamp column
    get_timestamp = udf(
        lambda x: datetime.fromtimestamp(x/1000).strftime('%Y-%m-%d %H:%M:%S'))
    df = df.withColumn('timestamp', get_timestamp(df.ts))
    
    # create datetime column from original timestamp column
    get_datetime = udf(
        lambda x: datetime.fromtimestamp(x/1000).strftime('%Y-%m-%d'))
    df = df.withColumn('datetime', get_datetime(df.ts))
    
    # extract columns to create time table
    time_table = df.select(df.timestamp.alias('start_time').cast(TimestampType()),
                           hour(df.timestamp).alias('hour'),
                           dayofmonth(df.timestamp).alias('day'),
                           weekofyear(df.timestamp).alias('week'),
                           month(df.timestamp).alias('month'),
                           year(df.timestamp).alias('year'),
                           date_format(df.timestamp, 'EEEE').alias('weekday')
                          ).dropDuplicates()
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy(
        'year','month').parquet(output_data + 'time', mode='overwrite')

    # read in song data to use for songplays table
    song_df = spark.read.json(input_data + 'song_data/*/*/*/*.json')

    # extract columns from joined song and log datasets to create songplays table
    songplays_table = df.join(song_df, how='left',
        on=[df.song == song_df.title, df.artist == song_df.artist_name]).select(
            df.timestamp.alias('start_time').cast(TimestampType()),
            df.userId.alias('user_id'),
            df.level,
            song_df.song_id,
            song_df.artist_id,
            df.sessionId.alias('session_id'),
            df.location,
            df.userAgent.alias('user_agent'),
            date_format(df.timestamp, 'y').alias('year'),
            date_format(df.timestamp, 'M').alias('month'))

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy(
        'year','month').parquet(output_data + 'songplays', mode='overwrite')


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://dengr-data-lakes/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)

    spark.stop()

if __name__ == "__main__":
    main()
