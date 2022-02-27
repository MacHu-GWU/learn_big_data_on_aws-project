# -*- coding: utf-8 -*-

"""
Online E-Commerce Order Data
"""

import uuid
import random
from typing import Dict, List
from datetime import datetime, timedelta, timezone

import pandas as pd
from rich import print as rprint
from faker import Faker
from s3pathlib import S3Path, context

from ..config import config
from ..boto_ses import boto_ses
from .base import Dataset as DS, FormatEnum

context.attach_boto_session(boto_ses)

fake = Faker()

genders = [0, 1]


class Dataset(DS):
    _dimension_tables_created = False

    def create_dimension_tables(self):
        if self._dimension_tables_created is True:
            return

        self.n_customer = 1000
        self.customers: Dict[int: dict] = dict()
        for customer_id in range(1, 1 + self.n_customer):
            customer = {
                "customer_id": customer_id,
                "email": fake.email(),
                "name": fake.name(),
                "dob": fake.date(),
                "gender": random.choice(genders),
                "billing_address": fake.address(),
                "shipping_address": fake.address(),
            }
            self.customers[customer_id] = customer

        self.n_item = 300
        self.items: Dict[int: dict] = dict()
        for item_id in range(1, 1 + self.n_item):
            item = {
                "item_id": item_id,
                "name": fake.word(),
                "price": 0.01 * random.randint(30, 10000),
            }
            self.items[item_id] = item

    def create_one(self, nth_file: int, **kwargs):
        self.create_dimension_tables()
        n_order_per_day_lower = 30
        n_order_per_day_upper = 100

        start_date = datetime(2022, 1, 1, tzinfo=timezone.utc)
        item_ids = list(range(1, 1 + self.n_item))

        today_date = start_date + timedelta(days=(nth_file - 1))
        orders: List[dict] = list()
        for _ in range(random.randint(n_order_per_day_lower, n_order_per_day_upper)):
            order = {
                "order_id": str(uuid.uuid4()),
                "create_time": (today_date + timedelta(seconds=random.randint(0, 86399))),
                "customer": self.customers[random.randint(1, self.n_customer)],
            }
            items = list()
            for item_id in random.sample(item_ids, random.randint(1, 5)):
                item = self.items[item_id]
                item["quantity"] = random.randint(1, 10)
                items.append(item)
            order["items"] = items
            orders.append(order)
        df = pd.DataFrame(orders)
        s3path = S3Path(
            self.s3path_loc,
            f"year={today_date.year}",
            f"month={str(today_date.month).zfill(2)}",
            f"day={str(today_date.day).zfill(2)}",
            f"{str(1).zfill(3)}.json"
        )
        print(f"create {s3path.basename}, console_url = {s3path.console_url}")
        with s3path.open("w") as f:
            df.to_json(f, orient="records", lines=True)

    def create_all(
        self,
        **kwargs,
    ):
        print(f"--- creating dataset {self.s3path_loc.basename} ---")
        for nth_file in range(1, 1 + self.n_files):
            self.create_one(nth_file=nth_file)


dataset = Dataset(
    name="ds_003_walmart_mongodb",
    format=FormatEnum.json_multi_line,
    datalake_s3_loc=config.s3path_dataset_prefix,
    n_files=100,
    n_records_per_file=0,
)
