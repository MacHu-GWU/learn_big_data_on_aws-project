# -*- coding: utf-8 -*-

"""
注:

- 单个文件是一个跨越多行被格式化的大型 JSON 这种情况无法被 AWS Catalog 所收录.
"""

import json
from rich import print as rprint
from faker import Faker
from s3pathlib import S3Path, context
from mpire import WorkerPool

from ..config import config
from ..boto_ses import boto_ses
from .base import Dataset as DS, FormatEnum

context.attach_boto_session(boto_ses)


class Dataset(DS):
    def create_one(self, nth_file: int, **kwargs):
        fake = Faker()
        data = {
            "id": nth_file,
            "name": fake.name(),
        }
        s3path = S3Path(self.s3path_loc, f"{str(nth_file).zfill(3)}.json")
        print(f"create {s3path.basename}, console_url = {s3path.console_url}")
        with s3path.open("w") as f:
            json.dump(data, f, indent=4)

    def create_all(
        self,
        **kwargs,
    ):
        print(f"--- creating dataset {self.s3path_loc.basename} ---")
        for nth_file in range(1, 1 + self.n_files):
            self.create_one(nth_file=nth_file)


dataset = Dataset(
    name="ds_002_single_doc_json",
    format=FormatEnum.json_single_doc,
    datalake_s3_loc=config.s3path_dataset_prefix,
    n_files=10,
    n_records_per_file=1,
)
