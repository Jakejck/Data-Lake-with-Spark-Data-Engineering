import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = input_data + "song_data"    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    df.createOrReplaceTempView("song_data")
    songs_table = spark.sql('''
                      SELECT song_id,title,artist_id, year,duration 
                      FROM song_data
                      ''')
    
    # write songs table to parquet files partitioned by year and artist
    song_table.write.partitionBy('year','artist_id').mode("overwrite").parquet(output_data)

    # extract columns to create artists table
    artists_table = spark.sql('''
                        SELECT artist_id, artist_name, artist_location, artist_latitude, artist_longitude 
                        FROM song_data
                        ''')
    
    # write artists table to parquet files
    artists_table.write.mode("overwrite").parquet(output_data)


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = input_data + "log_data"

    # read log data file
    df = spark.read.json(song_data)
    df.createOrReplaceTempView("log_data")
    
    # filter by actions for song plays
    df = spark.sql('''
               SELECT * 
               FROM log_data
               WHERE page = 'NextSong'
               ''')

    # extract columns for users table    
    users_table = spark.sql('''
                        SELECT distinct userid,firstName,lastName,gender,level
                        FROM log_data
                        ''')
    
    # write users table to parquet files
    users_table.write.mode("overwrite").parquet(output_data)

    # create timestamp column from original timestamp column
    # get_timestamp = udf()
    # df = 
    
    spark.udf.register("get_hour", lambda x:int(datetime.datetime.fromtimestamp(x/1000.0).hour))
    spark.udf.register("get_year", lambda x:int(datetime.datetime.fromtimestamp(x/1000.0).year))
    spark.udf.register("get_day", lambda x:int(datetime.datetime.fromtimestamp(x/1000.0).day))
    spark.udf.register("get_month", lambda x:int(datetime.datetime.fromtimestamp(x/1000.0).month))
    spark.udf.register("get_week", lambda x:int(datetime.datetime.fromtimestamp(x/1000.0).week))
    
    # create datetime column from original timestamp column
    # get_datetime = udf()
    # df = 
    
    # extract columns to create time table
    time_table = spark.sql('''
          SELECT from_unixtime(ts/1000, 'yyyy-MM-dd HH:mm:ss') as start_time,
          get_hour(ts) as hour,
          get_day(ts) as day,
          weekofyear(from_unixtime(ts/1000, 'yyyy-MM-dd')) as week,
          get_month(ts) as month,
          get_year(ts) as year,
          dayofweek(from_unixtime(ts/1000, 'yyyy-MM-dd')) as weekday
          FROM log_data 
          '''
          )
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year','month').mode("overwrite").parquet(output_data)

    # read in song data to use for songplays table
    song_df = input_data + "song_data"
    df = spark.read.json(song_df)
    song_df.createOrReplaceTempView("song_data")

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql('''
               SELECT from_unixtime(a.ts/1000, 'yyyy-MM-dd HH:mm:ss') AS start_time, 
               a.userId AS user_id,a.level, b.song_id, b.artist_id,a.sessionId,a.location,a.userAgent,
               get_year(a.ts) as year, get_month(a.ts) as month
               FROM log_data AS a LEFT JOIN song_data AS b
               ON a.artist = b.artist_name AND a.song = b.title
               WHERE a.page = 'NextSong'
               ''')

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('year','month').mode("overwrite").parquet(output_data)


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://emrtest111/test/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
