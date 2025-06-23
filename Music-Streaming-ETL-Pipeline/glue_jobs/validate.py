import pandas as pd
import boto3
import io
import sys

# S3 config
raw_base = "s3://music-streaming-data-02/raw"
validated_base = "s3://music-streaming-data-02/validated"
bad_base = "s3://music-streaming-data-02/bad-records"
bucket = "music-streaming-data-02"