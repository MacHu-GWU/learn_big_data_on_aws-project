# -*- coding: utf-8 -*-

import os
import json
import copy
import random

from rich import print as rprint
from faker import Faker
from s3pathlib import S3Path, context
from mpire import WorkerPool

from ..config import config
from ..boto_ses import boto_ses
from .base import Dataset, FormatEnum

context.attach_boto_session(boto_ses)


# class DataSet(Dataset):
#     def _cr


dataset = Dataset(
    name="ds_001_multi_line_json",
    format=FormatEnum.json_multi_line,
    datalake_s3_loc=config.s3path_dataset_prefix,
    n_files=10,
    n_records_per_file=50,
)



s3path_prefix = S3Path(config.s3path_prefix, "ds01_nested_json")

fake = Faker()

n_files = 50
n_message_lower = 1
n_message_upper = 3
n_tag_lower = 0
n_tag_upper = 5


def create_one(nth_file: int):
    data = {
        "id": nth_file,
        "messages": [
            f"message {id + 1}"
            for id in range(random.randint(n_message_lower, n_message_upper))
        ],
        "tags": [
            {"key": "Name", "value": fake.name()}
            for _ in range(random.randint(n_tag_lower, n_tag_upper))
        ]
    }
    data["data"] = copy.copy(data)

    s3path = S3Path(s3path_prefix, f"{str(nth_file).zfill(3)}.json")
    print(f"dump to {s3path.console_url}")
    with s3path.open("w") as f:
        json.dump(data, f, indent=4)

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
