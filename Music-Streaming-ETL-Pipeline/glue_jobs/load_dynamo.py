import os
import boto3
import logging
import pyarrow.dataset as ds
import pyarrow.fs as fs
import posixpath
import re
from botocore.exceptions import ClientError