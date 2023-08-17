# -*- coding: utf-8 -*-

"""
此脚本用于探索读取 Parquet 文件的最佳策略. 其中包含了创建测试数据集, 以及用不同的方式读取,
测量时间和内存消耗的代码.

Conclusion:

- For local parquet file:
- 对于本地文件 polars.scan_parquet > polars.read_parquet > pandas.read_parquet
    > pandas.read_parquet + use row group filter. 并且 scan_parquet 跟其他几个相比
    存在数量级的差距. 这是因为 scan_parquet
        使用了 lazy load 的策略.
- s3_client.get_object 是单线程, 在个人电脑上大约是 5-10MB / S, 而 s3_client.download_file
    是多线程多个 CPU 一起下载, 大约几个核心速度就是原来的几倍. 但是多线程 download 时每个线程
    下载的最小 chunk 是 8MB, 也就是说你的文件要大于 8MB * CPU 核心数才能跑满带宽.
- s3path.open, smart_open, s3fs.open 这些都是用的单线程, 和 s3_client.get_object 一样.
- 如果先将文件用 s3_client.download_file 下载到本地, 再进行读取的速度会比直接从 S3 读取要快很多,
    因为目前没有多线程创建多个 buffer 来读取 S3 的方法. 如果你用 s3_client.download_fileobj
    方法, 你还能直接将数据写入 buffer 中从而避免了将文件写入磁盘的过程, 速度会更快.
- AWS Lambda 读取 S3 的速度大约是 75MB - 100MB / S, 比个人电脑快多了.
- AWS Lambda (10GB 内存) 从 S3 上下载 500MB 文件 (多线程) 的速度大约是 6.5 秒.
- 用上面的技巧用 AWS Lambda 读 500MB parquet file 并用 filter 的速度大约是 6.5 秒,
    也就是说 parse 数据以及 filter 的速度跟 IO 相比可以忽略不计.
"""

import typing as T
import os
import io
import math
import dataclasses
from functools import cached_property

# 注意, 一般 pl 和 pandas 在 Lambda 上只能 2 选 1. 两个都安装很容易超过 250MB 依赖的限制.
import numpy as np
import pandas as pd
import polars as pl
from pathlib_mate import Path
from s3pathlib import S3Path, context
from boto_session_manager import BotoSesManager
from fixa.timer import DateTimeTimer

dir_here = Path.dir_here(__file__)
IS_LAMBDA = "AWS_LAMBDA_FUNCTION_NAME" in os.environ


@dataclasses.dataclass
class Config:
    """
    :param aws_profile: the aws profile you want to use
    :param n_col: number of columns in the test dataframe
    :param n_row: number of rows in the test dataframe
    :param row_group_size: number of rows in each parquet row group
    :param n_row_group: number of row groups in the parquet file
    """

    n_col: int = dataclasses.field()
    n_row: int = dataclasses.field()
    row_group_size: int = dataclasses.field()
    fname: str = dataclasses.field()
    aws_profile: T.Optional[str] = dataclasses.field(default=None)

    @property
    def n_row_group(self):
        return int(math.ceil(self.n_row / self.row_group_size))

    @cached_property
    def bsm(self):
        return BotoSesManager(profile_name=self.aws_profile)

    @property
    def s3dir_root(self) -> S3Path:
        return S3Path(
            f"s3://{self.bsm.aws_account_id}-{self.bsm.aws_region}-data"
            "/projects/explore_the_read_parquet_best_practice_using_polars/"
        ).to_dir()

    @property
    def s3path(self) -> S3Path:
        return self.s3dir_root.joinpath(f"{self.fname}.snappy.parquet")

    @property
    def path(self) -> Path:
        if IS_LAMBDA:
            return Path("/tmp").joinpath(f"{self.fname}.snappy.parquet")
        else:
            return dir_here.joinpath(f"{self.fname}.snappy.parquet")

    def show(self):
        print("--- Project settings")
        print(f"aws_profile = {self.aws_profile}")
        print(f"n_col = {self.n_col}")
        print(f"n_row = {self.n_row}")
        print(f"row_group_size = {self.row_group_size}")
        print(f"n_row_group = {self.n_row_group}")
        print(f"file size = {self.fname}")
        print(f"preview s3 file at: {self.s3path.console_url}")
        print(f"preview local file at: file://{self.path}")


def timeit(n: int, func):
    """
    Measure a callable function's average execution time. The function must
    return a number (elapsed seconds).
    """
    lst = list()
    for _ in range(n):
        lst.append(func())
    lst.sort()
    if len(lst) >= 3:
        lst = lst[1:-1]  # ignore the highest and the lowest value
    elapse = "%.6f" % (sum(lst) / len(lst),)
    print(f"{n} times average elapse = {elapse}")


def create_test_data():
    df = pl.from_numpy(
        np.random.randint(
            1,
            1000000,
            (config.n_row, config.n_col),
        ),
        schema=[f"col_{i}" for i in range(1, 1 + config.n_col)],
    )
    with config.path.open("wb") as f:
        df.write_parquet(f, compression="snappy", row_group_size=config.row_group_size)
    print(f"file size = {config.path.size_in_text}")
    config.s3path.upload_file(config.path, overwrite=True)


def _polars_read_parquet(f):
    df = pl.read_parquet(
        f,
        columns=["col_1", "col_2"],
    ).filter(pl.col("col_1") <= 1000)
    _ = df.shape
    # print(df.shape)


def _polars_scan_parquet(uri: str):
    df = (
        pl.scan_parquet(
            uri,
        )
        .select(
            "col_1",
            "col_2",
        )
        .filter(pl.col("col_1") <= 1000)
        .collect()
    )
    _ = df.shape
    # print(df.shape)


def _pandas_read_parquet_then_filter(f):
    df = pd.read_parquet(f, columns=["col_1", "col_2"])
    df = df[df["col_1"] <= 1000]
    # print(df.shape)


def _pandas_read_parquet_use_filter_while_reading(f):
    df = pd.read_parquet(
        f,
        columns=["col_1", "col_2"],
        filters=[
            ("col_1", "<=", 1000),
        ],
    )
    # print(df.shape)


def download_s3_file():
    with DateTimeTimer(
        # display=True, # show info
        display=False,  # mute
    ) as timer:
        config.path.remove_if_exists()
        config.bsm.s3_client.download_file(
            config.s3path.bucket, config.s3path.key, str(config.path)
        )
    return timer.elapsed


def polars_read_parquet():
    with DateTimeTimer(
        # display=True, # show info
        display=False,  # mute
    ) as timer:
        _polars_read_parquet(str(config.path))
    return timer.elapsed


def polars_scan_parquet():
    with DateTimeTimer(
        # display=True, # show info
        display=False,  # mute
    ) as timer:
        _polars_scan_parquet(str(config.path))
    return timer.elapsed


def pandas_read_parquet_then_filter():
    with DateTimeTimer(
        # display=True, # show info
        display=False,  # mute
    ) as timer:
        _pandas_read_parquet_then_filter(str(config.path))
    return timer.elapsed


def pandas_read_parquet_use_filter_while_reading():
    with DateTimeTimer(
        # display=True, # show info
        display=False,  # mute
    ) as timer:
        _pandas_read_parquet_use_filter_while_reading(str(config.path))
    return timer.elapsed


def polars_scan_parquet_from_s3_download_fileobj():
    with DateTimeTimer(
        # display=True, # show info
        display=False,  # mute
    ) as timer:
        buffer = io.BytesIO()
        config.bsm.s3_client.download_fileobj(
            config.s3path.bucket,
            config.s3path.key,
            buffer,
        )
        f = io.BytesIO(buffer.getvalue())
        df = (
            pl.read_parquet(
                f,
            )
            .select(
                "col_1",
                "col_2",
            )
            .filter(pl.col("col_1") <= 1000)
        )
        _ = df.shape
        # print(df.shape)
    return timer.elapsed


def pandas_scan_parquet_from_s3_download_fileobj():
    with DateTimeTimer(
        # display=True, # show info
        display=False,  # mute
    ) as timer:
        buffer = io.BytesIO()
        config.bsm.s3_client.download_fileobj(
            config.s3path.bucket,
            config.s3path.key,
            buffer,
        )
        f = io.BytesIO(buffer.getvalue())
        df = pd.read_parquet(
            f,
            columns=["col_1", "col_2"],
            filters=[
                ("col_1", "<=", 1000),
            ],
        )
        _ = df.shape
        # print(df.shape)
    return timer.elapsed


# --- measure
def measure_download_s3_file():
    print("--- measure_download_s3_file")
    timeit(3, download_s3_file)


def measure_polars_read_parquet_from_local():
    print("--- measure_polars_read_parquet_from_local")
    timeit(10, polars_read_parquet)


def measure_polars_scan_parquet_from_local():
    print("--- measure_polars_scan_parquet_from_local")
    timeit(10, polars_scan_parquet)


def measure_pandas_read_parquet_then_filter():
    print("--- measure_pandas_read_parquet_then_filter")
    timeit(10, pandas_read_parquet_then_filter)


def measure_pandas_read_parquet_use_filter_while_reading():
    print("--- measure_pandas_read_parquet_use_filter_while_reading")
    timeit(10, pandas_read_parquet_use_filter_while_reading)


def measure_polars_scan_parquet_from_s3_download_fileobj():
    print("--- measure_polars_scan_parquet_from_s3_download_fileobj")
    timeit(3, polars_scan_parquet_from_s3_download_fileobj)


def measure_pandas_scan_parquet_from_s3_download_fileobj():
    print("--- measure_pandas_scan_parquet_from_s3_download_fileobj")
    timeit(3, pandas_scan_parquet_from_s3_download_fileobj)


# ------------------------------------------------------------------------------
# measure benchmark
# ------------------------------------------------------------------------------
if IS_LAMBDA:
    aws_profile = None
else:
    aws_profile = "awshsh_app_dev_us_east_1"
config_5mb = Config(
    aws_profile=aws_profile,
    n_col=10,
    n_row=100000,
    row_group_size=10000,
    fname="5MB",
)

config_50mb = Config(
    aws_profile=aws_profile,
    n_col=10,
    n_row=1000000,
    row_group_size=100000,
    fname="50MB",
)

config_500mb = Config(
    aws_profile=aws_profile,
    n_col=10,
    n_row=10000000,
    row_group_size=1000000,
    fname="500MB",
)

# config = config_5mb
# config = config_50mb
config = config_500mb

context.attach_boto_session(config.bsm.boto_ses)

config.show()
print("--- Benchmark result")


def lambda_handler(event=None, context=None):
    # create_test_data()
    # measure_download_s3_file()
    measure_polars_read_parquet_from_local()
    measure_polars_scan_parquet_from_local()
    # measure_pandas_read_parquet_then_filter()
    # measure_pandas_read_parquet_use_filter_while_reading()
    # measure_polars_scan_parquet_from_s3_download_fileobj()
    # measure_pandas_scan_parquet_from_s3_download_fileobj()

lambda_handler()
