import pandas as pd
import boto3
import io
import sys

# S3 config
raw_base = "s3://music-streaming-data-02/raw"
validated_base = "s3://music-streaming-data-02/validated"
bad_base = "s3://music-streaming-data-02/bad-records"
bucket = "music-streaming-data-02"

# Required columns
required_users = ['user_id', 'user_name', 'user_age', 'user_country', 'created_at']
required_songs = ['track_id', 'track_name', 'track_genre', 'duration_ms']
required_streams = ['user_id', 'track_id', 'listen_time']

s3 = boto3.client("s3")