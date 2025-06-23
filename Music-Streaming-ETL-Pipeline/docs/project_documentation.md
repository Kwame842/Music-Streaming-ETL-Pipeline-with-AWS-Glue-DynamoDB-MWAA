# Music Streaming ETL Pipeline - Project Documentation

## Overview

This project implements an end-to-end ETL pipeline for processing music streaming data using AWS-native services such as AWS Glue, Amazon S3, DynamoDB, Amazon Athena, and Amazon Managed Workflows for Apache Airflow (MWAA). The pipeline validates raw data, computes analytics/KPIs, and loads aggregated results into DynamoDB.

---

## Project Structure

```
music-streaming-etl-pipeline/
├── dags/
│   └── stream_metrics_pipeline.py
├── glue_jobs/
│   ├── validation/
│   │   └── glue_validation.py
│   ├── transform/
│   │   └── glue_transform.py
│   └── load/
│       └── load_dynamo.py
├── stepfunctions/
│   └── etl_flow.json
├── scripts/
│   └── create_processedstreams_table.py
├── configs/
│   └── job_configs.json
├── requirements/
│   └── requirements.txt
├── README.md
└── docs/
    └── project_documentation.md
```

---

## Components

### 1. Data Sources (S3)

- **Bucket:** `music-streaming-data-02`
- **Folders:**

  - `raw/users/users.csv`
  - `raw/songs/songs.csv`
  - `raw/streams/` (contains multiple CSVs)

### 2. Glue Validation Job (`glue_validation.py`)

- Validates required columns for `users`, `songs`, and `streams`.
- Cleans rows with nulls.
- Saves validated data to `validated/` folder in Parquet format.
- Logs bad records in `bad-records/` by category.

### 3. Glue Transform Job (`glue_transform.py`)

- Loads validated data.
- Joins streams with song metadata.
- Computes:

  - Daily Genre KPIs (unique listeners, total listening time, average time)
  - Top 3 Songs per Genre per Day
  - Top 5 Genres per Day

- Outputs saved under `processed/` in respective folders.

### 4. Glue Load Job (`load_dynamo.py`)

- Reads processed data from `processed/avg_metrics` and `processed/top_songs`.
- Automatically creates DynamoDB tables `DailyGenreKPIs` and `TopSongsPerGenre` if not present.
- Handles both partitioned and non-partitioned Parquet datasets.
- Skips bad rows and logs any insertion issues.

### 5. MWAA DAG (`stream_metrics_pipeline.py`)

- Watches for new stream files in `raw/streams/`.
- Checks if the stream file has been processed using `ProcessedStreams` DynamoDB table.
- Triggers validation, transform, and load jobs in Glue.
- Archives stream files after successful processing.

### 6. Step Function Workflow (`etl_flow.json`)

- Optional orchestration for Spark jobs and Glue crawlers.
- Executes jobs and starts crawlers in parallel.

### 7. Scripts (`create_processedstreams_table.py`)

- One-off script to create the `ProcessedStreams` DynamoDB table used for tracking.

### 8. Configs (`job_configs.json`)

- Can be used to externalize Glue job names, paths, or schema information.

---

## Data Flow Summary

```
S3 (raw/) --> Glue Validation Job --> S3 (validated/)
        --> Glue Transform Job --> S3 (processed/)
                                  --> Glue Load Job --> DynamoDB
```

---

## DynamoDB Tables

1. **ProcessedStreams**

   - Tracks processed stream files.
   - Keys: `filename`, `status`, `timestamp`

2. **DailyGenreKPIs**

   - PartitionKey: `date`
   - SortKey: `genre`
   - Attributes: listen_count, unique_listeners, total_listening_time, avg_listening_time

3. **TopSongsPerGenre**

   - PartitionKey: `date`
   - SortKey: `genre`
   - Attributes: track_id, play_count, rank

---

## Requirements

```
boto3
pyarrow
pyspark
aws-glue
```

Add these to `requirements/requirements.txt` for local development or packaging.

---

## Best Practices

- Keep Glue jobs modular: validate → transform → load.
- Use job parameters (`--S3_OUTPUT_PATH`, `--RAW_KEY`, etc.) for flexibility.
- Archive raw data after ingestion.
- Use consistent schema names across files.
- Monitor job execution via MWAA or Step Functions.

---

## Monitoring and Logging

- Each Glue job uses Python logging for information and error tracking.
- MWAA logs are available in CloudWatch.
- Validation script saves bad records to `bad-records/` for audit.
- DAG ensures one-time file processing using DynamoDB.

---

## Future Enhancements

- Enable Athena for querying `validated/` and `processed/`.
- Integrate notifications (SNS or Slack) on job failures.
- Add versioning to S3 and DynamoDB tables.
- Schedule DAG for periodic batch jobs.

---

## Contributors

- Data/ETL Engineer: Kwame A. Boateng
- Cloud Infra: AWS
- Tools: Glue, S3, Athena, DynamoDB, Airflow, Step Functions

---

## License

MIT License or as per company policy.

---

## Appendix

- [x] All paths parameterized
- [x] MWAA orchestrates the whole flow
- [x] Supports re-processing prevention
- [x] Fully serverless (if no EMR/Step Functions used)
