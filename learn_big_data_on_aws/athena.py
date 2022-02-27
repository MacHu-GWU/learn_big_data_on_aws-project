# -*- coding: utf-8 -*-

from s3pathlib import S3Path
from pyathena import connect
from prettytable import from_db_cursor

from .config import config
from .boto_ses import boto_ses

# define the s3 foldter to store result
s3path_athena_result = S3Path(config.bucket, config.athena_result_prefix)

# define connection, use AWS CLI named profile for authentication
conn = connect(
    s3_staging_dir=s3path_athena_result.uri,
    session=boto_ses,
)

def run_sql(sql: str):
    cur = conn.cursor()
    print(from_db_cursor(cur.execute(sql)))
