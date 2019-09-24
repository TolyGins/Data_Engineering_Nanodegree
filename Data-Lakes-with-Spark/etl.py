import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from schemas import log_schema


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config.get('AWS', 'AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY']=config.get('AWS','AWS_SECRET_ACCESS_KEY')


def create_spark_session():
    """Creates or retrives a Spark session """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark



def process_song_data(spark, input_data, output_data):
    """Reads json data from S3 then processes it and writes it back to S3 with appropreate partitions
    
        Parameters:
            spark       : Spark Session
            input_data  : location of song_data files
            output_data : S3 bucket were dimensional tables in parquet format will be stored
                    
    """
    
    
    # get filepath to song data file
    song_data = "song_data/A/A/A/TRAAAAK128F9318786.json"
    song_data = input_data+song_data
    
    
    # read song data file
    df = spark.read.json(song_data)
    
    df.show(5)

    # create a SQL TEMP Table 
    df.createOrReplaceTempView("songs")
    songDF = spark.sql("""
                    SELECT DISTINCT
                    song_id,
                    title,
                    artist_id,
                    year,
                    duration
                    FROM songs
                    """)

    # extract columns to create songs table
    print ('song table schema :')
    songDF.show(5, truncate = False)
    

    # write songs table to parquet files partitioned by year and artist
    print ('writing song_data :')
    songDF.write.mode('overwrite').partitionBy('year', 'artist_id').parquet(output_data + 'song_data')
    print('success!')
    
    # extract columns to create artists table
    df.createOrReplaceTempView("artist")
    artistDF = spark.sql("""
                    SELECT DISTINCT
                    artist_id,
                    artist_name AS name,
                    artist_location AS location,
                    artist_latitude AS latitude,
                    artist_longitude AS longitude
                    FROM artist
                    """)

    print ('artist table schema :')
    artistDF.show(5, truncate = False)
    
    
    # write artists table to parquet files
    print ('writing artist data:')
    artistDF.write.mode('overwrite').parquet(output_data +'artist_data')
    print ('success!')


    
    
def process_log_data(spark, input_data, output_data):
    """Reads log json data from S3 using a schema that I defined. The function then processes that data, joins it with the processed song_data and writes it back to S3 with appropreate partitions
    
       Parameters:
            spark       : Spark Session
            input_data  : Location of log data files
            output_data : S3 bucket where processed data will be stored in parque files
                    
    """
    
    # get filepath to log data file
    log_data = "log_data/*/*"
    log_data = input_data+log_data

    # read log data file
    df = spark.read.json(log_data, schema = log_schema, mode="DROPMALFORMED")
    # filter by actions for song plays
    df_filter = df.filter(df.page == 'NextSong')
    
    # extract columns for users table 
    df_filter.createOrReplaceTempView("users")
    
    
    usersDF = spark.sql("""
                   SELECT DISTINCT
                    cast (userId AS INT) as userId
                    , firstName AS first_name
                    , lastName AS last_name
                    , gender 
                    , level 
                    FROM users
                    WHERE
                    userId IS NOT NULL
                    """)
    
    print ('users table schema:')
    usersDF.show(10, truncate = False)       
  
    
    # write users table to parquet files
    print ('writing user_data :')
    usersDF.write.mode('overwrite').parquet(output_data + 'user_data')
    print ('success!')

    # extract columns to create time table
    df_filter.createOrReplaceTempView("time")

    timeDF = spark.sql("""
                    SELECT 
                    from_unixtime (cast(ts as BIGINT)/1000) as start_time
                    , hour (from_unixtime (cast(ts as BIGINT)/1000)) AS hour
                    , dayofmonth(from_unixtime (cast(ts as BIGINT)/1000)) AS day
                    , weekofyear(from_unixtime (cast(ts as BIGINT)/1000)) AS week
                    , month(from_unixtime (cast(ts as BIGINT)/1000)) AS month
                    , year(from_unixtime (cast(ts as BIGINT)/1000)) AS year
                    , date_format(from_unixtime (cast(ts as BIGINT)/1000), 'EEEE') AS weekday 
                    FROM time
                    """)
    
    print ('time table schema:')
    timeDF.show(10, truncate = False)   
    
    # write time table to parquet files partitioned by year and month
    print ('writing time data:')
    timeDF.write.mode('overwrite').partitionBy('year', 'month').parquet(output_data + 'time_data')
    print ('success!')

    # read in song data to use for songplays table
    song_df = spark.read.parquet(output_data + 'song_data')

    # extract columns from joined song and log datasets to create songplays table 
    df_filter.createOrReplaceTempView("logs")
    
    song_df.createOrReplaceTempView ("songs")
    
    joinedDF =  spark.sql("""
                        SELECT 
                        from_unixtime (cast(ts as BIGINT)/1000) AS start_time
                        , userId AS user_id
                        , level
                        , song_id
                        , artist_id
                        , sessionId AS session_id
                        , location
                        , userAgent
                        , year(from_unixtime (cast(ts as BIGINT)/1000)) AS year
                        , month(from_unixtime (cast(ts as BIGINT)/1000)) AS month
                        FROM logs as l
                        INNER JOIN songs AS s
                            ON l.song = s.title
                            AND 
                            CAST (s.duration AS float) = cast (l.length AS float)
                        WHERE
                        userId IS NOT NULL
                        """)

    print ('schema for songplay table:')
    joinedDF.show(10, truncate=False)

    # write songplays table to parquet files partitioned by year and month
    print ('writing songplay data:')
    joinedDF.write.mode('overwrite').partitionBy('year', 'month').parquet(output_data + 'songplay_data')
    print ('success!')


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://data-bucket-demo-udac/processed_data/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
