from airflow import DAG
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from airflow.exceptions import AirflowSkipException
import boto3
import datetime

# Constants
S3_BUCKET = "music-streaming-data-02"
STREAM_PREFIX = "raw/streams/"
REGION = "us-east-1"
DDB_TRACKING_TABLE = "ProcessedStreams"