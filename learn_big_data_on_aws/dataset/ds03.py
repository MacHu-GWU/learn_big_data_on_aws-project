# -*- coding: utf-8 -*-

import os
import json
import copy
import random

from rich import print as rprint
from faker import Faker
from s3pathlib import S3Path, context
from mpire import WorkerPool
import pandas as pd
import awswrangler as wr

from learn_aws_glue_etl_programming.config import config
from learn_aws_glue_etl_programming.boto_ses import boto_ses

context.attach_boto_session(boto_ses)

s3path_prefix = S3Path(config.s3path_prefix, "ds02_nested_parquet")

fake = Faker()

n_files = 3
n_rows_per_file = 1000
n_message_lower = 1
n_message_upper = 3
n_tag_lower = 0
n_tag_upper = 5


def create_one(nth_file: int):
    start_id = 1 + (nth_file - 1) * n_rows_per_file
    end_id = start_id + n_rows_per_file
    data = [
        {
            "id": _id,
            "messages": [
                f"message {id + 1}"
                for id in range(random.randint(n_message_lower, n_message_upper))
            ],
            "tags": [
                {"key": "Name", "value": fake.name()}
                for _ in range(random.randint(n_tag_lower, n_tag_upper))
            ]
        }
        for _id in range(start_id, end_id)
    ]
    for dct in data:
        dct["data"] = copy.deepcopy(dct)

    df = pd.DataFrame(data, columns=["id", "messages", "tags", "data"])
    s3path = S3Path(s3path_prefix, f"{str(nth_file).zfill(3)}.parquet")
    print(f"dump to {s3path.console_url}")
    wr.s3.to_parquet(
        df,
        path=s3path.uri,
        index=False,
        dataset=False,
    )

    return data


def create_dataset():
    print(f"--- creating dataset {s3path_prefix.basename} ---")
    kwargs = [
        {"nth_file": ith_file}
        for ith_file in range(1, 1 + n_files)
    ]
    with WorkerPool(n_jobs=4) as pool:
        pool.map(create_one, kwargs)


if __name__ == "__main__":
    # create_one(nth_file=1)
    create_dataset()
    pass
