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
