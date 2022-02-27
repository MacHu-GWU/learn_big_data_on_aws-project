# -*- coding: utf-8 -*-

import pandas as pd
from rich import print as rprint
from faker import Faker
from s3pathlib import S3Path, context

from ..config import config
from ..boto_ses import boto_ses
from .base import Dataset as DS, FormatEnum

context.attach_boto_session(boto_ses)

fake = Faker()


class Dataset(DS):
    def create_one(self, nth_file: int, **kwargs):
        start_id = (nth_file - 1) * self.n_records_per_file + 1
        end_id = start_id + self.n_records_per_file
        data = [
            {
                "id": id,
                "name": fake.name(),
            }
            for id in range(start_id, end_id)
        ]
        df = pd.DataFrame(data)
        s3path = S3Path(self.s3path_loc, f"{str(nth_file).zfill(3)}.json")
        print(f"create {s3path.basename}, console_url = {s3path.console_url}")
        with s3path.open("w") as f:
            df.to_json(f, orient="records", lines=True)

    def create_catalog(self):
        sts_client = boto_ses.client("sts")
        account_id = sts_client.get_caller_identity()["Account"]
        glue_client = boto_ses.client("glue")
        kwargs = dict(
            CatalogId=account_id,
            DatabaseName=config.dbname,
            TableInput=dict(
                Name=self.name,
                StorageDescriptor=dict(
                    Columns=[
                        dict(
                            Name="id",
                            Type="int",
                        ),
                        dict(
                            Name="name",
                            Type="string",
                        )
                    ],
                    Location=self.s3path_loc.uri,
                    InputFormat="TextInputFormat",
                    OutputFormat="IgnoreKeyTextOutputFormat",
                    Compressed=False,
                    SerdeInfo=dict(
                    )
                )
            )
        )
        rprint(account_id)

dataset = Dataset(
    name="ds_001_multi_line_json",
    format=FormatEnum.json_multi_line,
    datalake_s3_loc=config.s3path_dataset_prefix,
    n_files=10,
    n_records_per_file=50,
)
