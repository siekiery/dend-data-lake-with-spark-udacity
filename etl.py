import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, desc, row_number, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, dayofweek, date_format, to_timestamp, to_date
from pyspark.sql.window import Window


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config.get('USER', 'AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY']=config.get('USER', 'AWS_SECRET_ACCESS_KEY')


def create_spark_session():
    """Starts a spark session"""
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """Instructions to ETL song data. Reads song data from input_data S3 bucket into a pyspark dataframe. Selects and transforms columns into songs and artists tables exported as parquet files on output_data S3 bucket."""
    
    # get filepath to song data file
    song_data = f"{input_data}/song_data/*/*/*/*.json"
    
    # read song data file
    df = spark.read.json(song_data)
    print("Song data load to df")
    
    # extract columns to create songs table
    songs_table = df.select(["song_id", "title", "artist_id", "year", "duration", "artist_name"])
    print("Song table selected")
    
    # drop duplicates from songs table
    songs_table = songs_table.dropDuplicates(["song_id"])
    print("Song table duplicates removed")
    
    print("Song table writing to parquet")
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_name").parquet(f"{output_data}/songs_table.parquet")
    print("Writing to parquet completed!")
    
    # extract columns to create artists table
    artists_table = df.select(["artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"]) 
    print("Artists table selected")
    
    # rename columns dropping artist prefix
    artists_table = artists_table \
                        .withColumnRenamed("artist_name", "name") \
                        .withColumnRenamed("artist_location", "location") \
                        .withColumnRenamed("artist_latitude", "latitude") \
                        .withColumnRenamed("artist_longitude", "longitude")
    
    print("Artist table columns renamed")
                                 
                                             
    # drop duplicates from artist table
    artists_table = artists_table.dropDuplicates(["artist_id"])
    print("Artists table duplicates removed")
    
    print("Artists table writing to parquet")
    # write artists table to parquet files
    artists_table.write.parquet(f"{output_data}/artists_table.parquet")
    print("Writing to parquet completed!")

def process_log_data(spark, input_data, output_data):
    """Instructions to ETL log data. Reads log data from input_data S3 bucket into a pyspark dataframe. Selects and transforms columns into users, time and songplays tables exported as parquet files on output_data S3 bucket. The songplay table uses columns from both log data and song data sources."""
    
    # get filepath to log data file
    log_data = f"{input_data}/log_data/*/*/*.json"
    
    # read log data file
    df = spark.read.json(log_data)
    print("Log data load to df")
    
    # filter by actions for song plays
    df = df.filter(df.page == "NextSong")
    print("Log df filtered NextSong only")
    
    # rename columns
    df = df \
            .withColumnRenamed("firstName", "first_name") \
            .withColumnRenamed("lastName", "last_name") \
            .withColumnRenamed("sessionId", "session_id") \
            .withColumnRenamed("userAgent", "user_agent") \
            .withColumnRenamed("userId", "user_id")
    
    print("Log df columns renamed")

    # extract columns for users table    
    users_table = df.select(["user_id", "first_name", "last_name", "gender", "level", "ts"])
    print("Users table selected")
    
    print("Opening window on users table")
    # ensure the latest information on the user is kept while removing duplicates                                 
    # Define a window specification to
    window_spec = Window.partitionBy("user_id").orderBy(desc("ts"))

    # Add a row number column based on the window specification
    users_table = users_table.withColumn("row_number", row_number().over(window_spec))

    # Filter the DataFrame to keep only the rows with row_number = 1
    users_table = users_table.filter(users_table.row_number == 1)

    # Drop the row_number column
    users_table = users_table.drop(*["row_number", "ts"])
    print("Window completed")
    
    print("Users table writing to parquet")
    # write users table to parquet files
    users_table.write.parquet(f"{output_data}/users_table.parquet")
    print("Writing to parquet completed!")
    
    print("Extract from datetime starting")
    # create timestamp column from original timestamp column
    # get_timestamp = udf(lambda x: datetime.timestamp(x)) # amend udf
    # df = df.withColumn("start_time", get_timestamp(df.ts))
    df = df.withColumn("start_time", to_timestamp(df.ts))
    
    # create datetime column from original timestamp column
    #get_datetime = udf(lambda x: datetime.fromtimestamp(x / 1000.0))
    # df = df.withColumn("datetime", get_datetime(df.ts))
    df = df.withColumn("datetime", to_date(df.start_time))
    
    # extract from datetime
    df = df \
            .withColumn("hour", hour(df.datetime)) \
            .withColumn("day", dayofmonth(df.datetime)) \
            .withColumn("week", weekofyear(df.datetime)) \
            .withColumn("month", month(df.datetime)) \
            .withColumn("year", hour(df.datetime)) \
            .withColumn("weekday", dayofweek(df.datetime))
    
    print("Extract from datetime completed")
    
    # extract columns to create time table
    time_table = df.select(["start_time", "hour", "day", "week", "month", "year", "weekday"])
    print("Time table selected")
    
    # drop duplicates from time table 
    time_table = time_table.dropDuplicates(["start_time"])
    print("Time table duplicates removed")
    
    print("Time table writing to parquet")
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month").parquet(f"{output_data}/time_table.parquet")
    print("Writing to parquet completed!")
    
    # get filepath to song data file
    song_data = f"{input_data}/song_data/*/*/*/*.json"
    
    # read in song data to use for songplays table
    song_df = spark.read.json(song_data)
    
    # in song df rename year column to avoid ambiguity
    song_df = song_df.withColumnRenamed("year", "album_year")

    # Create songplay df by joining both tables
    songplay_df = df.join(song_df, [(df.song == song_df.title), (df.artist == song_df.artist_name)], "left")
    
    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = songplay_df.select(["start_time", "user_id", "level", "song_id", "artist_id", "session_id", "location", "user_agent", "year", "month"])
    
    # add songplay_id column with unique id
    songplays_table = songplays_table.withColumn("songplay_id", monotonically_increasing_id())
    
    print("Songplay table writing to parquet")
    
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year", "month").parquet(f"{output_data}/songplays_table.parquet")
    print("Writing to parquet completed!")

def main():
    """Main ETL body. Starts spark session and runs ETL procedures. Reads song_data and log_data from S3, transforms them to create five different tables, and writes them to partitioned parquet files in table directories on S3."""
    print("> Starting")
    spark = create_spark_session()
    print("> Spark session created")
    
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://jp-udacity-datalake"
    
    print("> Processing song data")
    process_song_data(spark, input_data, output_data)
    
    print("> Processing log data")
    process_log_data(spark, input_data, output_data)
    
    print("> Finished!")

if __name__ == "__main__":
    main()

    

