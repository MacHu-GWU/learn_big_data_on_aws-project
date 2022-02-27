# -*- coding: utf-8 -*-

from s3pathlib import S3Path


class Config:
    aws_profile = "aws_data_lab_sanhe"
    aws_region = "us-east-2"
    # where you store data, artifacts
    bucket = "aws-data-lab-sanhe-for-everything-us-east-2"
    # s3 folder for data lake
    dataset_prefix = "poc/2022-02-26-learn_big_data_on_aws/dataset"
    # s3 folder for athena results
    athena_result_prefix = "athena/results"
    # glue catalog database name
    dbname = "poc"

    @property
    def s3path_dataset_prefix(self):
        return S3Path(self.bucket, self.dataset_prefix)

    @property
    def s3path_athena_result_prefix(self):
        return S3Path(self.bucket, self.athena_result_prefix)



config = Config()
