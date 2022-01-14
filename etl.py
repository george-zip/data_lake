import os
import configparser
import sys

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, dayofweek, to_timestamp
from pyspark.sql.types import StructType, StructField as Fld, DoubleType as Dbl, StringType as Str, \
    IntegerType as Int, LongType as Lng

"""
Functions for extracting records from json files in s3 and populating cleaned and transformed tables 
"""

config = configparser.ConfigParser()
config.read("dl.cfg")

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


def get_json_from_s3(spark: SparkSession, data_path: str, schema: StructType, mode: str) -> DataFrame:
    """
    Load json file(s) from s3_path, validate using schema and return DataFrame
    :param spark: SparkSession
    :param data_path: Valid path NOT including URI or prefix (s3a:// for example)
    :param schema: Schema describing field names and types
    :param mode: Configuration mode
    :return: DataFrame containing records in json file
    """
    path = config.get(mode, "READ_PREFIX") + data_path
    print(f"Retrieving data files from {path}")
    return spark.read.schema(schema) \
        .option("recursiveFileLookup", "true").json(path)


def process_song_data(spark: SparkSession, mode: str) -> (DataFrame, DataFrame):
    """
    Load json song data from S3 into staging table and move to final tables
    :param spark: Created Spark session
    :param mode: Configuration mode
    :return: Tuple containing two final DataFrame tables
    """
    songs_data_staging = get_json_from_s3(
        # TODO: Update to full path (this worked  "udacity-dend/song-data/A/*/*",)
        spark, "udacity-dend/song_data/",
        StructType([Fld("artist_id", Str()), Fld("artist_latitude", Dbl()), Fld("artist_longitude", Dbl()),
                    Fld("artist_location", Str()), Fld("artist_name", Str()), Fld("song_id", Str()),
                    Fld("title", Str()), Fld("duration", Dbl()), Fld("year", Int())]),
        mode
    )

    print(f"{songs_data_staging.count()} song staging records loaded")

    # extract columns to create songs table
    songs_table = songs_data_staging.select("song_id", "title", "artist_id", "year", "duration")

    print(f"{songs_table.count()} song records populated")

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").mode(config.get(mode, "WRITE_MODE")) \
        .parquet("s3a://wallerstein-udacity/data_lake/songs_table")

    # extract columns to create artists table
    artists_table = songs_data_staging.select("artist_id", "artist_name", "artist_location", "artist_latitude",
                                              "artist_longitude")

    print(f"{artists_table.count()} artist records populated")

    # write artists table to parquet files
    artists_table.write.mode(config.get(mode, "WRITE_MODE"))\
        .parquet("s3a://wallerstein-udacity/data_lake/artists_table")

    return songs_table, artists_table


def process_log_data(spark: SparkSession, songs_table: DataFrame, mode: str) -> (DataFrame, DataFrame, DataFrame):
    """
    Load log file from s3 into staging table then populate users, time and song_play tables
    :param spark: SparkSession object
    :param songs_table: DataFrame containing songs records
    :param mode: Configuration mode
    :return: Tuple containing users, time and songplays tables
    """
    log_data_staging = populate_log_staging_table(spark, mode)
    users_table = populate_user_table(log_data_staging, mode)
    time_table = populate_time_table(spark, log_data_staging, mode)
    songplays_table = populate_songplays_table(log_data_staging, songs_table, time_table, mode)
    return users_table, time_table, songplays_table


def populate_songplays_table(log_data_staging: DataFrame, songs_table: DataFrame, time_table: DataFrame,
                             mode: str) -> DataFrame:
    """
    Populate songplays time from join of staging and songs tables
    Persist to s3 partitioned by year and month
    :param log_data_staging: Staging table
    :param songs_table: Songs table
    :param time_table: Time table
    :param mode: Configuration mode
    :return: songplays table
    """
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
    print(f"{songplays_table.count()} songplay records populated")
    # write to s3 partitioned by year and month
    songplays_table.join(time_table, on=["start_time"], how="inner").write.mode(config.get(mode, "WRITE_MODE")) \
        .partitionBy("year", "month").parquet("s3a://wallerstein-udacity/data_lake/song_plays")
    return songplays_table


def populate_time_table(spark: SparkSession, log_data_staging: DataFrame, mode: str) -> DataFrame:
    """
    Generate correct time and date fields, populate time table
    Write to s3 and return
    :param spark: SparkSession
    :param log_data_staging: Log staging table
    :param mode: Configuration mode
    :return: Populated time table
    """

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
    print(f"{time_table.count()} time records populated")
    # write to s3
    time_table.write.mode(config.get(mode, "WRITE_MODE")).partitionBy("year", "month") \
        .parquet("s3a://wallerstein-udacity/data_lake/time_table")
    return time_table


def populate_user_table(log_data_staging: DataFrame, mode: str) -> DataFrame:
    """
    Create user table with field names matching the rest of the schema,
    populate from staging and write to s3 in parquet format
    :param log_data_staging: Log staging table
    :param mode: Configuration mode
    :return: Populated user table
    """
    users_table = log_data_staging.select(
        log_data_staging.userId.cast("int").alias("user_id"),
        log_data_staging.firstName.alias("first_name"),
        log_data_staging.lastName.alias("last_name"),
        log_data_staging.gender.alias("gender"),
        log_data_staging.level.alias("level")
    ).dropDuplicates()
    print(f"{users_table.count()} user records populated")
    users_table.write.mode(config.get(mode, "WRITE_MODE")).parquet("s3a://wallerstein-udacity/data_lake/users_table")
    return users_table


def populate_log_staging_table(spark: SparkSession, mode: str) -> DataFrame:
    """
    Populate log staging table from s3 and return
    :param spark: SparkSession
    :param mode: Configuration mode
    :return: Populated log staging table
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
    log_data_staging = get_json_from_s3(spark, "udacity-dend/log_data/", log_schema, mode).filter("page = 'NextSong'")
    print(f"{log_data_staging.count()} songplay log records loaded")
    return log_data_staging


def main() -> None:
    """
    Create spark session and start table import process
    :return: None
    """
    # Set running mode for reading configuration options
    mode = "DEFAULT" if len(sys.argv) == 1 else sys.argv[1]

    spark = create_spark_session()
    # set Spark logging level
    spark.sparkContext.setLogLevel(config.get(mode, "LOG_LEVEL"))

    songs_table, _ = process_song_data(spark, mode)
    process_log_data(spark, songs_table, mode)

    print("SUCCESS!")


if __name__ == "__main__":
    main()
