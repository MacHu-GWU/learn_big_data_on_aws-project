# -*- coding: utf-8 -*-

import typing as T
import dataclasses
from functools import cached_property

from boto_session_manager import BotoSesManager
from s3pathlib import S3Path


@dataclasses.dataclass
class Config:
    aws_profile: T.Optional[str] = dataclasses.field(default=None)

    @cached_property
    def bsm(self) -> BotoSesManager:
        return BotoSesManager(profile_name=self.aws_profile)

    @cached_property
    def s3dir_root(self) -> S3Path:
        return S3Path(
            f"s3://{self.bsm.aws_account_id}-{self.bsm.aws_region}-data"
            f"/projects/learn_big_data_on_aws/"
        ).to_dir()

    @cached_property
    def s3dir_athena_result(self) -> S3Path:
        return S3Path(
            f"s3://{self.bsm.aws_account_id}-{self.bsm.aws_region}-data"
            f"/athena/results/"
        ).to_dir()

    @property
    def glue_database(self) -> str:
        return "learn_big_data_on_aws"


config = Config(
    aws_profile="awshsh_app_dev_us_east_1",
)
