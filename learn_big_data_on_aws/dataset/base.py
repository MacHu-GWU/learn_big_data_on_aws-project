# -*- coding: utf-8 -*-

import enum
from s3pathlib import S3Path
from mpire import WorkerPool


class FormatEnum(enum.Enum):
    csv = "csv"
    tsv = "tsv"
    json_multi_line = "json_multi_line"
    json_single_doc = "json_single_doc"
    parquet = "parquet"


class Dataset:
    def __init__(
        self,
        name: str,
        format: FormatEnum,
        datalake_s3_loc: S3Path,
        n_files: int,
        n_records_per_file: int,
        **kwargs,
    ):
        self.name = name
        self.format = format
        self.datalake_s3_loc = datalake_s3_loc
        self.n_files = n_files
        self.n_records_per_file = n_records_per_file

    @property
    def s3path_loc(self):
        """
        S3 folder for this dataset
        """
        return S3Path(self.datalake_s3_loc, self.name + "/")

    def create_one(
        self,
        nth_file: int,
        **kwargs,
    ) -> S3Path:
        raise NotImplementedError

    def create_all(
        self,
        **kwargs,
    ):
        print(f"--- creating dataset {self.s3path_loc.basename} ---")
        kwargs = [
            {"nth_file": ith_file}
            for ith_file in range(1, 1 + self.n_files)
        ]
        with WorkerPool() as pool:
            pool.map(self.create_one, kwargs)

    def delete_all(self):
        self.s3path_loc.delete_if_exists()
