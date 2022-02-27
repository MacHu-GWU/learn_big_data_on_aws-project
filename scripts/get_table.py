# -*- coding: utf-8 -*-

from rich import print as rprint
from learn_big_data_on_aws.boto_ses import glue_client, account_id
from learn_big_data_on_aws.config import config

res = glue_client.get_table(
    CatalogId=account_id,
    DatabaseName="learn_aws_glue_etl_programming",
    Name="ds01_nested_json",
)
rprint(res)