Explore the Read Parquet Best Practice Using Polars
==============================================================================
Keywords: Read, Parquet, Python, Polars, AWS, S3, Lambda


目标
------------------------------------------------------------------------------
Parquet 作为主流大数据存储格式, 它的 Columnar data format 和 Row Group 的两个特性可以大大提升读性能. 这里对这两个性能做一个简单的介绍:

- Columnar data format: 在磁盘中一列中的数据都是顺序存储的, 所以你如果只需要数据集中的指定列, 你可以只读取这一列的数据, 而不需要读取其他列的数据. 这样可以大大减少磁盘 IO. 此外还有为数据类型相同的一列数据进行压缩等特点这里我们不展开说了, 因为它们主要是为了节省空间, 而不是节约读性能.
- Row group: 在磁盘中数据被按照行分为了 Row Group (RG), 每一个 RG 在操作系统的磁盘上就是一个 Page. 而一个 RG 中包含了每个 Column 的统计信息, 例如最大和最小值. 这样如果你读数据的时候需要根据值进行 filter, 那么也可以跳过很多的 Row Group 从而大大减少了磁盘 IO.

本文档对 Parquet 的读操作进行了详细的测试, 希望能探索出最佳的读取 Parquet 文件的方法. 本文的探索主要针对位于本地磁盘上的 Parquet 文件, 和位于 AWS S3 上的 Parquet.


实验设计
------------------------------------------------------------------------------
我们创建了一个测试数据集. 由于在大数据领域, 一个 Parquet 文件通常推荐保持在 100 MB ~ 1GB 之间, 一个 Row Group 通常在 64MB 左右. 所以我们创建了如下数据集:

- 有 10M 行, 10 列.
- 每一列都是 1 - 1M 的随机整数.
- 数据分为 10 个 Row Group, 每个行组有 1M 行数据.
- 数据经过 snappy 压缩后约 500MB.

我们希望读取 col 1 和 col2 (col 3 - 10 不要), 并只返回 col 1 的值在 1 ~ 1000 之间的结果.

这个文件我们一份放在了本地磁盘上, 一份放在了 AWS S3 上, 用于模拟生产环境中通过网络 IO 的应用场景.

- 本地电脑室 MacBook M1 Pro, 32 G 内存, 10 核 CPU. 对本地文件的读取用 Mac 来进行.
- 为了减少网络的影响, 我们使用了 Lambda Function 来进行 S3 中的数据的读取. 因为 Lambda Function 会被部署在和 S3 同一个 Region 的网络中, 并且通信走的是内网, 所以网络速度非常稳定, 减少了测量误差, 也更接近生产环境的情况.

我们使用了 polars, 和 pandas + pyarrow 两套常见的数据分析工具来进行测试.


学到的经验
------------------------------------------------------------------------------
- 对于本地文件, polars.scan_parquet > polars.read_parquet > pandas.read_parquet > pandas.read_parquet + use row group filter. 速度级都在 0.1 秒以内. 其中 scan_parquet 达到了 0.02 秒. 并且 scan_parquet 跟其他几个相比存在数量级的差距. 这是因为 scan_parquet 使用了 lazy load 的策略.
- s3_client.get_object 是单线程, 在个人电脑上大约是 5-10MB / S, 而 s3_client.download_file 是多线程多个 CPU 一起下载, 速度要快很多.
- s3path.open, smart_open, s3fs.open 这些都是用的单线程
- 先将文件用 s3_client.download_file 下载到本地, 再进行读取的速度会比直接从 S3 读取要快很多, 因为目前没有多线程创建多个 buffer 来读取 S3 的方法.
- AWS Lambda 读取 S3 的速度大约是 75MB - 100MB / S, 比个人电脑快多了.
- 如果用 AWS Lambda 来写 ETL 程序, 可以先将文件用 download_file 下载到 /tmp, 然后再进行读取. Lambda 默认有 500 MB 磁盘大小的限制, 你可以将这个限制提高到最多 10G, 完全足够用了.


代码
------------------------------------------------------------------------------
用于探索的 Python 脚本.

.. literalinclude:: ./benchmark_test.py
   :language: python
   :linenos:

依赖列表.

.. literalinclude:: ./requirements.py
   :language: python
   :linenos:
