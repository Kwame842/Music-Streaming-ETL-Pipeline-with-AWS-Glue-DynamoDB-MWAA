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