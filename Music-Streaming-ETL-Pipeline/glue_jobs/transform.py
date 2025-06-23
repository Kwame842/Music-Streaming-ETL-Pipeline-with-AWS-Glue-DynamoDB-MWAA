# glue_transform.py
from awsglue.context import GlueContext
from awsglue.transforms import *
from pyspark.context import SparkContext
from pyspark.sql.functions import col, to_date, countDistinct, sum as spark_sum, count, row_number
from pyspark.sql.window import Window