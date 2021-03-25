import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek
from pyspark.sql.types import TimestampType


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Return a spark session instance.
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Process the songs data files with Spark and extract and create songs/artist table data from it.
    --------
    Param:
        spark: A spark session instance.
        input_data: input data path.
        output_data: output data path.
    Return:
        None.
    """
    # get filepath to song data file
    song_data = os.path.join(input_data, 'song_data/*/*/*/*.json')
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select('song_id', 'title', 'artist_id', 'year', 'duration').drop_duplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year', 'artist_id').mode('overwrite').parquet(output_data + 'songs/')

    # extract columns to create artists table
    artists_table = df.selectExpr('artist_id', 'artist_name as name', 'artist_location as location', 'artist_lattitude as lattitude', 'artist_longitude as longitude').drop_duplicates()
    
    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(output_data + 'artists/')


def process_log_data(spark, input_data, output_data):
    """
    Process the event log file and extract data for table time, users and songplays with Spark.
    --------
    Param:
        spark: A spark session instance.
        input_data: input data path.
        output_data: output data path.
    Return:
        None.
    """
    # get filepath to log data file
    log_data = os.path.join(input_data, 'log_data/')

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table    
    users_table = df.selectExpr('userId as user_id', 'firstName as first_name', 'lastName as last_name', 'gender', 'level').drop_duplicates()
    
    # write users table to parquet files
    users_table.write.mode('overwrite').parquet(output_data + 'users/')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.utcfromtimestamp(int(x)/1000), TimestampType())
    df = df.withColumn('timestamp', get_timestamp('ts'))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp(x), TimestampType())
    df = df.withColumn('start_time', get_datetime('timestamp')) \
        .withColumn('hour', hour('start_time')) \ 
        .withColumn('day', dayofmonth('start_time')) \ 
        .withColumn('week', weekofyear('start_time')) \ 
        .withColumn('month', month('start_time')) \ 
        .withColumn('year', year('start_time')) \
        .withColumn('weekday', dayofweek('start_time'))
    
    # extract columns to create time table
    time_table = df.select('start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday').drop_duplicates()
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year', 'month').mode('overwrite').parquet(output_data + 'time/')

    # read in song data to use for songplays table
    song_df = spark.read.parquet(os.path.join(output_data, 'songs/*/*/*'))
    songs_logs_df = df.join(song_df, (df.song == song_df.title))
    artists_df = spark.read.parquet(os.path.join(output_data, 'artists'))
    songs_logs_artists_df = songs_logs_df.join(artists_df, (songs_logs_df.artist == artists_df.name))
    songplays_df = songs_logs_artists_df.join(time_table, (songs_logs_artists_df.start_time == time_table.start_time), 'left').drop(songs_logs_artists_df.year)
    
    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = songplays_df.select(
        monotonically_increasing_id().alias('songplay_id'), 
        col('start_time'), 
        col('userId').alias('user_id'),
        col('level'),
        col('song_id'),
        col('artist_id'),
        col('sessionId').alias('session_id'),
        col('location'),
        col('userAgent').alias('user_agent'),
        col('year'),
        col('month')).drop_duplicates()

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('year', 'month').mode('overwrite').parquet(output_data + 'songplays/')


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = ""
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
