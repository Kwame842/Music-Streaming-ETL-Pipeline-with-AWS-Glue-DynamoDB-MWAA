# glue_transform.py
from awsglue.context import GlueContext
from awsglue.transforms import *
from pyspark.context import SparkContext
from pyspark.sql.functions import col, to_date, countDistinct, sum as spark_sum, count, row_number
from pyspark.sql.window import Window

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Load cleaned data from validated location
streams_df = spark.read.parquet("s3://music-streaming-data-02/validated/streams")

# Load reference data from raw songs path
songs_df = spark.read.option("header", True).csv("s3://music-streaming-data-02/raw/songs/songs.csv")

# Preprocessing
streams_df = streams_df.withColumn("listen_date", to_date("listen_time"))
songs_df = songs_df.select("track_id", "track_name", "track_genre", "duration_ms")

# Join stream with songs
joined_df = streams_df.join(songs_df, on="track_id", how="inner")

# KPI Aggregation
kpi_df = joined_df.groupBy("listen_date", "track_genre").agg(
    countDistinct("user_id").alias("unique_listeners"),
    spark_sum("duration_ms").alias("total_listening_time"),
    count("track_id").alias("listen_count")
)

# Average Listening Time per User
avg_df = kpi_df.withColumn(
    "avg_listen_time_per_user",
    col("total_listening_time") / col("unique_listeners")
)

# Top 3 Songs per Genre per Day
window = Window.partitionBy("listen_date", "track_genre").orderBy(col("listen_count").desc())
joined_count = joined_df.groupBy("listen_date", "track_genre", "track_name") \
    .agg(count("*").alias("listen_count"))

top_songs = joined_count.withColumn("rank", row_number().over(window)).filter(col("rank") <= 3)

# Top 5 Genres per Day
genre_window = Window.partitionBy("listen_date").orderBy(col("listen_count").desc())
genre_count = joined_df.groupBy("listen_date", "track_genre") \
    .agg(count("*").alias("listen_count"))

top_genres = genre_count.withColumn("rank", row_number().over(genre_window)).filter(col("rank") <= 5)

# Write outputs to processed path
avg_df.write.mode("overwrite").parquet("s3://music-streaming-data-02/processed/avg_metrics")
top_songs.write.mode("overwrite").parquet("s3://music-streaming-data-02/processed/top_songs")
top_genres.write.mode("overwrite").parquet("s3://music-streaming-data-02/processed/top_genres")
