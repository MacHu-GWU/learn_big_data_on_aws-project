# -*- coding: utf-8 -*-

import boto3
from .config import config

boto_ses = boto3.session.Session(
    profile_name=config.aws_profile, region_name=config.aws_region,
)
sts_client = boto_ses.client("sts")
account_id = sts_client.get_caller_identity()["Account"]

glue_client = boto_ses.client("glue")
