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
    print(f" Triggered Glue job {job_name} with Run ID: {response['JobRunId']}")

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

# === DAG Definition ===

with DAG(
    dag_id="stream_metrics_pipeline",
    description="Stream ETL: Validate, Transform, Load to DynamoDB",
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=["streaming", "glue", "dynamodb"]
) as dag:

    start = DummyOperator(task_id="start")
    end = DummyOperator(task_id="end")

    wait_for_stream = S3KeySensor(
        task_id="wait_for_stream",
        bucket_name=S3_BUCKET,
        bucket_key="raw/streams/*.csv",
        wildcard_match=True,
        poke_interval=60,
        timeout=600
    )

    check_stream = PythonOperator(
        task_id="check_if_processed",
        python_callable=check_if_processed
    )

    validate_data = PythonOperator(
        task_id="validate_data",
        python_callable=trigger_glue_job,
        op_kwargs={
            "job_name": "validate_stream_data",  # Your Python Shell Glue job
        }
    )

    branch_validation = BranchPythonOperator(
        task_id="branch_on_validation",
        python_callable=route_on_validation
    )

    handle_validation_failure = DummyOperator(task_id="handle_validation_failure")

    transform = PythonOperator(
        task_id="transform_data",
        python_callable=trigger_glue_job,
        op_kwargs={
            "job_name": "transform_stream_data",  # Glue ETL job
        }
    )

    load_dynamo = PythonOperator(
        task_id="load_to_dynamodb",
        python_callable=trigger_glue_job,
        op_kwargs={
            "job_name": "load_dynamodb",  # Glue ETL job that includes table creation
            "script_args": {
                "--S3_OUTPUT_PATH": f"s3://{S3_BUCKET}/processed"
            }
        }
    )

    mark_complete = PythonOperator(
        task_id="mark_as_processed",
        python_callable=mark_processed
    )

    archive_stream = PythonOperator(
        task_id="archive_stream_file",
        python_callable=move_to_archive
    )