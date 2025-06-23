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

# === Utility functions ===

def get_next_stream_file():
    s3 = boto3.client('s3')
    response = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=STREAM_PREFIX)
    for obj in sorted(response.get('Contents', []), key=lambda x: x['LastModified']):
        key = obj['Key']
        if key.endswith('.csv') and not key.startswith("archive/"):
            return key
    raise ValueError("No stream files found.")

def trigger_glue_job(job_name, script_args=None, **kwargs):
    glue = boto3.client("glue", region_name=REGION)
    if script_args is None:
        script_args = {}
    response = glue.start_job_run(JobName=job_name, Arguments=script_args)
    print(f"✅ Triggered Glue job {job_name} with Run ID: {response['JobRunId']}")

def check_if_processed(**kwargs):
    key = get_next_stream_file()
    kwargs['ti'].xcom_push(key='stream_key', value=key)
    dynamodb = boto3.resource("dynamodb", region_name=REGION)
    table = dynamodb.Table(DDB_TRACKING_TABLE)
    if 'Item' in table.get_item(Key={'filename': key}):
        raise AirflowSkipException(f"{key} already processed")
    table.put_item(Item={
        'filename': key,
        'status': 'processing',
        'timestamp': datetime.datetime.utcnow().isoformat()
    })

def mark_processed(**kwargs):
    key = kwargs['ti'].xcom_pull(key='stream_key')
    dynamodb = boto3.resource("dynamodb", region_name=REGION)
    table = dynamodb.Table(DDB_TRACKING_TABLE)
    table.update_item(
        Key={'filename': key},
        UpdateExpression="SET #s = :s, #t = :t",
        ExpressionAttributeNames={"#s": "status", "#t": "timestamp"},
        ExpressionAttributeValues={":s": "processed", ":t": datetime.datetime.utcnow().isoformat()}
    )

def move_to_archive(**kwargs):
    key = kwargs['ti'].xcom_pull(key='stream_key')
    s3 = boto3.client('s3')
    archive_key = key.replace("raw/", "archive/")
    s3.copy_object(Bucket=S3_BUCKET, CopySource={'Bucket': S3_BUCKET, 'Key': key}, Key=archive_key)
    s3.delete_object(Bucket=S3_BUCKET, Key=key)

def route_on_validation(**kwargs):
    # if validations failed you can customize this further
    return "transform_data"