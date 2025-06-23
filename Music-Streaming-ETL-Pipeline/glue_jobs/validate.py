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

def read_csv_from_s3(s3_uri):
    bucket_name = s3_uri.split("/")[2]
    key = "/".join(s3_uri.split("/")[3:])
    obj = s3.get_object(Bucket=bucket_name, Key=key)
    return pd.read_csv(io.BytesIO(obj["Body"].read()))


def write_parquet_to_s3(df, path):
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False)
    bucket_name = path.split("/")[2]
    key = "/".join(path.split("/")[3:])
    s3.put_object(Bucket=bucket_name, Key=key, Body=buffer.getvalue())


def validate_and_clean(df, required, name):
    missing = [col for col in required if col not in df.columns]
    if missing:
        raise Exception(f"[ERROR] Missing columns in {name}: {missing}")
    print(f"[INFO] {name} schema validated.")

    bad = df[df[required].isnull().any(axis=1)]
    good = df.dropna(subset=required)

    if not bad.empty:
        print(f"[WARN] Found {len(bad)} bad rows in {name}. Logging to S3...")
        write_parquet_to_s3(bad, f"{bad_base}/{name}/bad_{name}.parquet")

    return good


try:
    print("[INFO] Loading CSVs from S3...")

    users_df = read_csv_from_s3(f"{raw_base}/users/users.csv")
    songs_df = read_csv_from_s3(f"{raw_base}/songs/songs.csv")
    streams_df = read_csv_from_s3(f"{raw_base}/streams/streams1.csv")

    print("[INFO] Validating and cleaning data...")

    clean_users = validate_and_clean(users_df, required_users, "users")
    clean_songs = validate_and_clean(songs_df, required_songs, "songs")
    clean_streams = validate_and_clean(streams_df, required_streams, "streams")

    print("[INFO] Writing cleaned data to S3...")

    write_parquet_to_s3(clean_users, f"{validated_base}/users/clean_users.parquet")
    write_parquet_to_s3(clean_songs, f"{validated_base}/songs/clean_songs.parquet")
    write_parquet_to_s3(clean_streams, f"{validated_base}/streams/clean_streams.parquet")

    print(" Done! Cleaned data written to 'validated/' and bad rows to 'bad-records/'")

except Exception as e:
    print(f" Job failed: {e}")
    sys.exit(1)