import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import udf, col, from_unixtime
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, dayofweek, to_timestamp, to_utc_timestamp
from pyspark.sql.types import StructType, StructField as Fld, DoubleType as Dbl, StringType as Str, \
    IntegerType as Int, LongType as Lng, TimestampType as TStamp

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config.get('CREDENTIALS', 'AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY'] = config.get('CREDENTIALS', 'AWS_SECRET_ACCESS_KEY')


def create_spark_session() -> SparkSession:
    """
    Create SparkSession object with Hadoop-aws jar loaded
    :return: SparkSession
    """
    return SparkSession \
        .builder \
        .appName("udacity_data_lake_etl") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.3") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("fs.s3a.access.key", os.environ['AWS_ACCESS_KEY_ID']) \
        .config("fs.s3a.secret.key", os.environ['AWS_SECRET_ACCESS_KEY']) \
        .getOrCreate()


def get_json_from_s3(spark: SparkSession, s3_path: str, schema: StructType) -> DataFrame:
    """
    Load json file(s) from s3_path, validate using schema and return DataFrame
    :param spark: SparkSession
    :param s3_path: Valid S3 path not including URI identifier (s3:// or s3a://)
    :param schema: Schema describing field names and types
    :return: DataFrame containing records in json file
    """
    return spark.read.schema(schema) \
        .option("recursiveFileLookup", "true").json("s3a://" + s3_path)


def process_song_data(spark: SparkSession) -> (DataFrame, DataFrame):
    """
    Load json song data from S3 into staging table and move to final tables
    :param spark: Created Spark session
    :return: Tuple containing two final DataFrame tables
    """
    songs_data_staging = get_json_from_s3(
        # TODO: Update to full path
        spark, "udacity-dend/song-data/A/A/",
        StructType([Fld("artist_id", Str()), Fld("artist_latitude", Dbl()), Fld("artist_longitude", Dbl()),
                    Fld("artist_location", Str()), Fld("artist_name", Str()), Fld("song_id", Str()),
                    Fld("title", Str()), Fld("duration", Dbl()), Fld("year", Int())])
    )

    print(f"{songs_data_staging.count()} song records loaded")

    # extract columns to create songs table
    songs_table = songs_data_staging.select("song_id", "title", "artist_id", "year", "duration").dropDuplicates()

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").mode("overwrite") \
        .parquet("s3a://wallerstein-udacity/data_lake/songs_table")

    # extract columns to create artists table
    artists_table = songs_data_staging.select("artist_id", "artist_name", "artist_location", "artist_latitude",
                                              "artist_longitude").dropDuplicates()

    # write artists table to parquet files
    artists_table.write.mode("overwrite").parquet("s3a://wallerstein-udacity/data_lake/artists_table")

    return songs_table, artists_table


def process_log_data(spark: SparkSession, songs_table: DataFrame) -> (DataFrame, DataFrame, DataFrame):
    """
    Load log file from s3 into staging table then populate users, time and song_play tables
    :param spark: SparkSession object
    :param songs_table: DataFrame containing songs records
    :return: None
    """
    log_schema = StructType([Fld("artist", Str()),
                             Fld("auth", Str()),
                             Fld("firstName", Str()),
                             Fld("gender", Str()),
                             Fld("itemInSession", Int()),
                             Fld("lastName", Str()),
                             Fld("length", Str()),
                             Fld("level", Str()),
                             Fld("location", Str()),
                             Fld("method", Str()),
                             Fld("page", Str()),
                             Fld("registration", Str()),
                             Fld("sessionId", Int()),
                             Fld("song", Str()),
                             Fld("status", Int()),
                             Fld("ts", Lng()),
                             Fld("userAgent", Str()),
                             Fld("userId", Str())])

    log_data_staging = get_json_from_s3(spark, "udacity-dend/log-data/", log_schema).filter("page = 'NextSong'")

    print(f"{log_data_staging.count()} songplay log records loaded")

    # create user table with field names matching the rest of the schema
    users_table = log_data_staging.select(
        log_data_staging.userId.cast("int").alias("user_id"),
        log_data_staging.firstName.alias("first_name"),
        log_data_staging.lastName.alias("last_name"),
        log_data_staging.gender.alias("gender"),
        log_data_staging.level.alias("level")
    ).dropDuplicates()

    users_table.write.mode("overwrite").parquet("s3a://wallerstein-udacity/data_lake/users_table")

    log_data_staging = log_data_staging.withColumn("ts1", log_data_staging.ts / 1000)
    log_data_staging = log_data_staging.withColumn("timestamp", to_timestamp(log_data_staging.ts1))

    # extract columns to create time table, retaining UTC time zone
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    time_table = log_data_staging.select(
        log_data_staging.ts.alias("start_time"),
        hour(log_data_staging.timestamp).alias("hour"),
        dayofmonth(log_data_staging.timestamp).alias("day"),
        weekofyear(log_data_staging.timestamp).alias("week"),
        month(log_data_staging.timestamp).alias("month"),
        year(log_data_staging.timestamp).alias("year"),
        dayofweek(log_data_staging.timestamp).alias("weekday"),
    ).dropDuplicates()
    spark.conf.unset("spark.sql.session.timeZone")

    # write to s3
    time_table.write.mode("overwrite").partitionBy("year", "month")\
        .parquet("s3a://wallerstein-udacity/data_lake/time_table")

    # read in song data to use for songplays table
    join_table = log_data_staging.join(
        songs_table, (log_data_staging.song == songs_table.title) & (log_data_staging.length == songs_table.duration),
        how="left"
    )

    songplays_table = join_table.select(
        join_table.ts.alias("start_time"),
        join_table.userId.cast("int").alias("user_id"),
        join_table.level,
        join_table.song_id,
        join_table.artist_id,
        join_table.sessionId.alias("session_id"),
        join_table.location,
        join_table.userAgent.alias("user_agent")
    ).dropDuplicates()

    # write to s3 partitioned by year and month
    songplays_table.join(time_table, on=["start_time"], how="inner").write.mode("overwrite")\
        .partitionBy("year", "month").parquet("s3a://wallerstein-udacity/data_lake/song_plays")

    return users_table, time_table, songplays_table


def main() -> None:
    """
    Create spark session and start table import process
    :return: None
    """
    spark = create_spark_session()
    # reduce Spark logging
    spark.sparkContext.setLogLevel("ERROR")

    songs_table, _ = process_song_data(spark)
    process_log_data(spark, songs_table)


if __name__ == "__main__":
    main()
