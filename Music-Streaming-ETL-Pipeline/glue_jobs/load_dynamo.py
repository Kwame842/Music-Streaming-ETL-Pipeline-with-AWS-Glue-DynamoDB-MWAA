import os
import boto3
import logging
import pyarrow.dataset as ds
import pyarrow.fs as fs
import posixpath
import re
from botocore.exceptions import ClientError


# Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration
REGION = os.environ.get("AWS_REGION", "us-east-1")
S3_BUCKET = "music-streaming-data-02"
BASE_PREFIX = "processed"
s3_fs = fs.S3FileSystem(region=REGION)

# DynamoDB client
dynamodb = boto3.resource("dynamodb", region_name=REGION)
client = boto3.client("dynamodb", region_name=REGION)

# ======= Utility Functions =======

def get_latest_partition_path(prefix: str) -> str:
    """Get latest date=yyyy-mm-dd partition under a prefix"""
    s3 = boto3.client("s3")
    result = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix)
    partitions = [
        obj["Key"].split("/")[2]
        for obj in result.get("Contents", [])
        if re.match(r"date=\d{4}-\d{2}-\d{2}", obj["Key"].split("/")[2])
    ]
    if not partitions:
        raise Exception(f"No partitioned data found under {prefix}")
    latest = sorted(partitions)[-1]
    return f"s3://{S3_BUCKET}/{prefix}{latest}/"

def create_table_if_not_exists(name, key_schema, attr_defs):
    try:
        client.describe_table(TableName=name)
        logger.info(f" DynamoDB table '{name}' exists.")
    except client.exceptions.ResourceNotFoundException:
        logger.info(f" Creating DynamoDB table: {name}")
        client.create_table(
            TableName=name,
            KeySchema=key_schema,
            AttributeDefinitions=attr_defs,
            BillingMode="PAY_PER_REQUEST"
        )
        waiter = client.get_waiter("table_exists")
        waiter.wait(TableName=name)
        logger.info(f" Table '{name}' created.")

def load_parquet_to_dynamo(table, s3_path, item_builder):
    logger.info(f" Loading from {s3_path}")
    try:
        dataset = ds.dataset(s3_path, format="parquet", filesystem=s3_fs)
        table_data = dataset.to_table().to_pydict()
        rows = [dict(zip(table_data.keys(), values)) for values in zip(*table_data.values())]
    except Exception as e:
        logger.error(f" Could not read Parquet from {s3_path}: {e}")
        return

    logger.info(f" Writing {len(rows)} items to {table.name}")
    count = 0
    with table.batch_writer() as batch:
        for row in rows:
            try:
                item = item_builder(row)
                if item:
                    batch.put_item(Item=item)
                    count += 1
            except Exception as ex:
                logger.warning(f" Skipped row due to error: {ex}")
    logger.info(f" {count} items written to {table.name}")


# ======= Table Definitions =======

create_table_if_not_exists(
    "DailyGenreKPIs",
    key_schema=[
        {"AttributeName": "date", "KeyType": "HASH"},
        {"AttributeName": "genre", "KeyType": "RANGE"}
    ],
    attr_defs=[
        {"AttributeName": "date", "AttributeType": "S"},
        {"AttributeName": "genre", "AttributeType": "S"}
    ]
)

create_table_if_not_exists(
    "TopSongsPerGenre",
    key_schema=[
        {"AttributeName": "date", "KeyType": "HASH"},
        {"AttributeName": "genre_track", "KeyType": "RANGE"}
    ],
    attr_defs=[
        {"AttributeName": "date", "AttributeType": "S"},
        {"AttributeName": "genre_track", "AttributeType": "S"}
    ]
)

create_table_if_not_exists(
    "TopGenresPerDay",
    key_schema=[
        {"AttributeName": "date", "KeyType": "HASH"},
        {"AttributeName": "genre", "KeyType": "RANGE"}
    ],
    attr_defs=[
        {"AttributeName": "date", "AttributeType": "S"},
        {"AttributeName": "genre", "AttributeType": "S"}
    ]
)

# ======= Builders =======

def build_genre_kpi(row):
    if not row.get("date") or not row.get("track_genre"):
        return None
    return {
        "date": str(row["date"]),
        "genre": row["track_genre"],
        "listen_count": int(row.get("listen_count", 0)),
        "unique_listeners": int(row.get("unique_listeners", 0)),
        "total_listening_time": float(row.get("total_listening_time", 0)),
        "avg_listen_time_per_user": float(row.get("avg_listen_time_per_user", 0))
    }

def build_top_song(row):
    if not row.get("date") or not row.get("track_genre") or not row.get("track_name"):
        return None
    return {
        "date": str(row["date"]),
        "genre_track": f"{row['track_genre']}#{row['track_name']}",
        "genre": row["track_genre"],
        "track_name": row["track_name"],
        "play_count": int(row.get("listen_count", 0)),
        "rank": int(row.get("rank", 0))
    }

def build_top_genre(row):
    if not row.get("date") or not row.get("track_genre"):
        return None
    return {
        "date": str(row["date"]),
        "genre": row["track_genre"],
        "listen_count": int(row.get("listen_count", 0)),
        "rank": int(row.get("rank", 0))
    }

# ======= Load Data =======

load_parquet_to_dynamo(
    dynamodb.Table("DailyGenreKPIs"),
    get_latest_partition_path(f"{BASE_PREFIX}/avg_metrics/"),
    build_genre_kpi
)

load_parquet_to_dynamo(
    dynamodb.Table("TopSongsPerGenre"),
    get_latest_partition_path(f"{BASE_PREFIX}/top_songs/"),
    build_top_song
)

load_parquet_to_dynamo(
    dynamodb.Table("TopGenresPerDay"),
    get_latest_partition_path(f"{BASE_PREFIX}/top_genres/"),
    build_top_genre
)

logger.info(" All data loaded to DynamoDB successfully.")
