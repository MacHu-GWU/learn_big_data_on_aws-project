# -*- coding: utf-8 -*-

import os
import uuid
import random

import numpy as np
import pandas as pd
import polars as pl
from faker import Faker
from mpire import WorkerPool
from fixa.timer import DateTimeTimer
from s3pathlib import S3Path, context
from boto_session_manager import BotoSesManager


bsm = BotoSesManager(profile_name="awshsh_app_dev_us_east_1")
context.attach_boto_session(bsm.boto_ses)
fake = Faker()

s3dir_root = S3Path(
    f"s3://{bsm.aws_account_id}-{bsm.aws_region}-data"
    "/projects/polars_benchmark_in_aws_lambda/"
).to_dir()
print(f"preview at: {s3dir_root.console_url}")


n_files = 100
n_rows = 100000


def generate_one_file(ith_file: int):
    print(f"working on {ith_file} th file")

    df = pl.DataFrame()
    for id in range(1, 1 + 5):
        col = f"col_{id}"
        df = df.with_columns(
            pl.Series(
                name=col,
                values=np.random.randint(1, 1000000, size=n_rows),
            )
        )

    for id in range(6, 6 + 5):
        col = f"col_{id}"
        df = df.with_columns(
            pl.Series(
                name=col,
                values=np.random.rand(n_rows),
            )
        )

    for id in range(11, 11 + 5):
        col = f"col_{id}"
        df = df.with_columns(
            pl.Series(
                name=col,
                values=[str(uuid.uuid4()) for _ in range(n_rows)],
            )
        )

    for id in range(16, 16 + 5):
        col = f"col_{id}"
        df = df.with_columns(
            pl.Series(
                name=col,
                values=[" ".join(fake.sentences()) for _ in range(n_rows)],
            )
        )

    for id in range(21, 21 + 5):
        col = f"col_{id}"
        start = "{}-{}-{}".format(
            random.randint(2001, 2020),
            random.randint(1, 12),
            random.randint(1, 28),
        )
        df = df.with_columns(
            pl.Series(
                name=col,
                values=pd.date_range(start=start, periods=n_rows, freq="S"),
            )
        )

    s3path = s3dir_root.joinpath(
        "parquet",
        f"{str(ith_file).zfill(9)}.snappy.parquet",
    )
    with s3path.open("wb") as f:
        df.write_parquet(
            f,
            compression="snappy",  # 60MB
            # compression="uncompressed", # 85MB
        )

    s3path = s3dir_root.joinpath(
        "csv",
        f"{str(ith_file).zfill(9)}.csv",
    )
    with s3path.open("wb") as f:
        df.write_csv(f, has_header=True)

    s3path = s3dir_root.joinpath(
        "json",
        f"{str(ith_file).zfill(9)}.json",
    )
    with s3path.open("wb") as f:
        df.write_ndjson(f)


kwargs = [{"ith_file": ith_file} for ith_file in range(1, 1 + n_files)]
with DateTimeTimer():
    with WorkerPool(n_jobs=os.cpu_count()) as pool:
        results = pool.map(generate_one_file, kwargs)
