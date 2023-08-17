# -*- coding: utf-8 -*-

from datetime import datetime
import awswrangler as wr


def lambda_handler(event, context):
    df_list = list()
    n = 1
    start = datetime.utcnow()
    for ith_file in range(1, 1 + n):
        print(f"read {ith_file} file")

        uri = (
            f"s3://807388292768-us-east-1-data"
            f"/projects/polars_benchmark_in_aws_lambda/parquet"
            f"/{str(ith_file).zfill(9)}.snappy.parquet"
        )
        df = wr.s3.read_parquet(uri)
        df_list.append(df)

        elapsed = int((datetime.utcnow() - start).total_seconds())
        print(f"  done, elapsed {elapsed} seconds")
        print(df.shape)


lambda_handler(None, None)
