import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


# config = configparser.ConfigParser()
# config.read('dl.cfg')

# os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
# os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """returns: Spark Session object
    
    Description: Factory function that creates Spark Session objects.
    
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Description: Function extracts the information from the given location,
                  data is split into separate tables and loaded in Parquet format into output location for 
                  future processing.
                  In the current use case input/output locations are S3 buckets

    Parameters: 
        spark:       Spark Session object
        input_data:  Location of raw data
        output_data: Location of where processed data will be stored
    """
    
    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'
    
    # read song data file
    df = spark.read.json(song_data)
    print("Song_data schema is: ", df.printSchema())
    
    # register view
    df.createOrReplaceTempView('song_data_table')
    
    # extract columns to create songs table
    songs_table = spark.sql("""
                                SELECT DISTINCT artist_id, song_id, title, year, artist_name, duration
                                FROM song_data_table
                                WHERE song_id IS NOT NULL
                            """)
    print("Songs Table schema is: ", songs_table.printSchema())
    
    print("First 5 row look like this:\n ", songs_table.show(5))
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode('overwrite').partitionBy(["year","artist_id"]).parquet(output_data + "song_table/")

    # extract columns to create artists table
    artists_table = spark.sql("""   
                                 SELECT artist_name, artist_id, artist_location, artist_latitude, artist_longitude
                                 FROM song_data_table
                                 WHERE artist_id IS NOT NULL
                              """)
    
    print("Artist Table schema is: ")
    print(artists_table.printSchema())
    
    print("First 5 rows look like this:\n ")
    print(artists_table.show(5))
    
    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(output_data + "artists_table/")


def process_log_data(spark, input_data, output_data):
    """
    Description: Function extracts the information from the given location,
                  data is split into separate tables and loaded in Parquet format into output location for 
                  future processing.
                  In the current use case input/output locations are S3 buckets.
                  Specific JOINs are made to create songplay data tables.

    Parameters: 
        spark:       Spark Session object
        input_data:  Location of raw data
        output_data: Location of where processed data will be stored
    """
    
    # get filepath to log data file
    #log_data = input_data + 'log_data/*.json'   # Locat lesting
    log_data = input_data + 'log_data/*/*/*.json'  # S3

    # read log data file
    log_df = spark.read.json(log_data)
    log_df.createOrReplaceTempView('log_data_table')
    
    # filter by actions for song plays
    log_df = spark.sql("""
                        SELECT * FROM log_data_table
                        WHERE page = 'NextSong'
                       """)

    # extract columns for users table    
    users_table = spark.sql("""
                                SELECT DISTINCT firstName, lastName, gender, userId 
                                FROM log_data_table
                                WHERE userId IS NOT NULL
                            """)
    
    print("Users Table schema is: ")
    print(users_table.printSchema())
    print("First 5 rows look like this:\n")
    print(users_table.show(5))
    
    # write users table to parquet files
    users_table.write.mode('overwrite').parquet(output_data + "users_table/")

    # create timestamp column from original timestamp column
    
    # spark.udf.register("get_ts", lambda x: datetime.fromtimestamp(x/1000).strftime('%Y-%m-%d %H:%M:%S.%f'))
    #get_timestamp = udf()
    #df = 
    ts_df = spark.sql("""SELECT *,to_timestamp(ts/1000) as start_time
                            FROM log_data_table""")
    ts_df.createOrReplaceTempView('timestamp_df')
    
    # extract columns to create time table
    time_table = spark.sql("""SELECT 
                                start_time,
                                hour(start_time) as hour,
                                dayofmonth(start_time) as day,
                                weekofyear(start_time) as week,
                                month(start_time) as month,
                                year(start_time) as year,
                                dayofweek(start_time) as weekday
                                FROM timestamp_df                          
                            """)
    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode('overwrite').partitionBy(["year","month"]).parquet(output_data + "time_table/")

    # read in song data to use for songplays table
    song_df = spark.read.parquet(output_data + 'song_table/')
    
    # IMPROVE: is better to pass existing df, as it was computed in previous function
    # This version follows the requirements
    
    # Register view if data needs to be read from parquet
    song_df.createOrReplaceTempView('song_table')
    
    
    
    # log_df.printSchema()
    # song_df.printSchema()
    # extract columns from joined song and log datasets to create songplays table 
    songplay_df = spark.sql("""SELECT 
                                    monotonically_increasing_id() as songplay_id, 
                                    log_data_table.userId as user_id,
                                    to_timestamp(log_data_table.ts/1000) as start_time,
                                    month(to_timestamp(log_data_table.ts/1000)) as month,
                                    year(to_timestamp(log_data_table.ts/1000)) as year,
                                    log_data_table.level as level,
                                    song_table.song_id as song_id,
                                    song_table.artist_id as artist_id,
                                    log_data_table.sessionId as session_id,
                                    log_data_table.location as location,
                                    log_data_table.userAgent as user_agent
                                FROM song_table 
                                JOIN log_data_table ON song_table.artist_name = log_data_table.artist 
                                                        AND song_table.title = log_data_table.song
                            """)
    
    print("Songplays Table schema is: ")
    print(songplay_df.printSchema())
    print("First 5 rows look like this:\n ")
    print(songplay_df.show(5))
    
    # write songplays table to parquet files partitioned by year and month
    songplay_df.write.mode('overwrite').partitionBy(["year","month"]).parquet(output_data + "songplay/")


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://sparkify-data-lake-tables/"
    # Uncomment to test with local data
    #input_data = ""
    #output_data = ""
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
