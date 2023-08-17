# -*- coding: utf-8 -*-

from datetime import datetime

import boto3
import polars as pl
from pathlib import Path
from s3pathlib import S3Path, context

boto_ses = boto3.session.Session()
context.attach_boto_session(boto_ses)
s3_client = boto_ses.client("s3")

path_tmp_parquet = Path("/tmp/temp.parquet")

def lambda_handler(event, context):
    df_list = list()
    n = 1
    start = datetime.utcnow()
    for ith_file in range(1, 1 + n):
        print(f"read {ith_file} file")
        s3path = S3Path(
            f"s3://807388292768-us-east-1-data"
            f"/projects/polars_benchmark_in_aws_lambda/parquet"
            f"/{str(ith_file).zfill(9)}.snappy.parquet"
        )
        path_tmp_parquet.unlink(missing_ok=True)
        s3_client.download_file(
            s3path.bucket,
            s3path.key,
            str(path_tmp_parquet),
        )
        df = pl.read_parquet(str(path_tmp_parquet))

        # with s3path.open("rb") as f:
        #     df = pl.read_parquet(f)

        df_list.append(df)

        elapsed = int((datetime.utcnow() - start).total_seconds())
        print(f"  done, elapsed {elapsed} seconds")
        # print(df.shape)


# lambda_handler(None, None)
