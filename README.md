# Music-Streaming-ETL-Pipeline-with-AWS-Glue-DynamoDB-MWAA

# Project Documentation: Music Streaming ETL Pipeline with AWS Glue, DynamoDB, and MWAA

## Overview

This project implements a fault-tolerant, compute-efficient, and memory-efficient ETL pipeline for processing music streaming data using AWS-native services. The pipeline performs validation, transformation, aggregation of KPIs, and ingestion into DynamoDB. The orchestration is done using Managed Workflows for Apache Airflow (MWAA).

---

## Folder Structure in S3

```
s3://music-streaming-data-02/
├── raw/
│   ├── users/users.csv
│   ├── songs/songs.csv
│   └── streams/
│       ├── streams1.csv
│       ├── streams2.csv
│       └── ...
├── validated/
│   ├── users/
│   ├── songs/
│   └── streams/
├── bad-records/
│   ├── users/
│   ├── songs/
│   └── streams/
├── processed/
    ├── avg_metrics/
    ├── top_songs/
    └── top_genres/
```

---

## Components and Responsibilities

### 1. Validation (Glue Python Shell Job)

**Script:** `validate.py`

* Loads raw data from S3.
* Validates required columns in each dataset.
* Cleans rows with missing values.
* Writes cleaned data to `validated/` folder.
* Writes bad records to `bad-records/` for auditing.

### 2. Transformation and Aggregation (Glue Spark Job)

**Script:** `transform.py`

* Loads validated stream data and song reference data.
* Joins datasets using track ID.
* Extracts `listen_date` from timestamp.
* Computes:

  * Average listening time per user per genre/date.
  * Top 3 songs per genre per date.
  * Top 5 genres per date.
* Outputs results as partitioned Parquet files under `processed/`.

### 3. DynamoDB Loader (Glue Python Shell Job)

**Script:** `load_dynamo.py`

* Reads Parquet files from `processed/`.
* Creates DynamoDB tables if not present:

  * `DailyGenreKPIs`
  * `TopSongsPerGenre`
* Converts and uploads data using `boto3` and `batch_writer()`.
* Handles both partitioned and non-partitioned datasets.
* Skips invalid rows and logs errors.

---

## DynamoDB Table Definitions

### `DailyGenreKPIs`

* Partition key: `date` (String)
* Sort key: `genre` (String)
* Attributes:

  * `listen_count`: Number
  * `unique_listeners`: Number
  * `total_listening_time`: Number
  * `avg_listening_time`: Number

### `TopSongsPerGenre`

* Partition key: `date` (String)
* Sort key: `genre` (String)
* Attributes:

  * `track_id`: String
  * `play_count`: Number
  * `rank`: Number

---

## Airflow DAG: `music_etl_pipeline.py`

* Uses `S3KeySensor` to wait for new stream data.
* Checks if file has already been processed (via `ProcessedStreams` DynamoDB table).
* Triggers Glue validation jobs for users, songs, and streams.
* Routes execution based on validation success.
* Triggers transformation and DynamoDB load jobs.
* Marks stream file as processed and archives it.

### Tasks:

* `wait_for_stream` – Waits for new CSVs in `raw/streams/`.
* `check_if_processed` – Avoids reprocessing the same file.
* `validate_*` – Runs validation Glue jobs.
* `transform_data` – Triggers the KPI aggregation job.
* `load_to_dynamodb` – Triggers DynamoDB loader.
* `mark_as_processed` – Updates tracking table.
* `archive_stream_file` – Moves file to `archive/`.

---

## Permissions

### IAM Roles

Ensure your Glue job role and Airflow execution role have permissions for:

* `s3:GetObject`, `s3:PutObject`, `s3:ListBucket`
* `dynamodb:PutItem`, `dynamodb:UpdateItem`, `dynamodb:DescribeTable`
* `glue:StartJobRun`, `glue:GetJobRun`

### Example Trust Policy for MWAA Role

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "airflow-env.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
```

---

## Notes and Tips

* Use `.dropna()` with required columns only to preserve rows with optional fields.
* Use `partitionBy("date")` to allow efficient querying from Athena or Redshift Spectrum.
* Validate column names exactly; Glue is case-sensitive.
* Use the `ResourceInUseException` exception to avoid trying to recreate existing tables.
* Archive files after they’re processed to prevent duplicates.

---

## Future Enhancements

* Add data quality scoring (e.g., % nulls).
* Trigger notifications (e.g., SNS) on validation failures.
* Automate table schema updates in Glue Data Catalog.
* Integrate Athena or Redshift Spectrum for querying KPIs.

---

## Conclusion

This ETL pipeline leverages AWS Glue, DynamoDB, and MWAA to efficiently process music streaming data with built-in validation, partitioning, fault tolerance, and automation. The design ensures extensibility, low maintenance, and cost-effective scalability.
