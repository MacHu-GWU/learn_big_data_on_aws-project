Learn Big Data On AWS Project
==============================================================================

1

了解你的目标
------------------------------------------------------------------------------
在 AWS 的 Big Data 生态中, 有这些相关的服务:

- 存储:
    - Data Store: AWS S3, 用于存储大量的原始数据文件, 或是处理后以大数据查询友好的格式 (例如 Parquet) 保存的数据文件.
    - Data warehouse: AWS Redshift, AWS 的高性能数据仓库, 能在秒级查询 PB 级别的数据.
- 分析:
    - Serverless SQL for S3: AWS Athena, 无需昂贵的数据仓库, 直接使用廉价的 S3 存储数据, 并直接对 S3 上的数据用 SQL 进行查询
- 管理:
    - Catalog: AWS Glue Catalog, 通用型的 Data Catalog, 能用来保存位于 AWS 上的数据集, 也能用来保存非 AWS 上的数据集, 例如 微软 Oracle 的数据库, 以及 Hadoop 集群上的 HDFS.
- 处理:
    - EMR: AWS 管理的 Hadoop 集群. 目前主流的大数据处理框架是 Spark.
    - ETL: AWS Glue ETL, serverless 的 Spark 集群.

对于创业公司来说, 使用 serverless 的服务会非常有吸引力, 免除了管理基础设施的麻烦. 由于在启动阶段业务的负载很不稳定, 很难按需配置基础设施, 配置少了不够用, 配置多了浪费钱. 并且对于个人用户学习来说, 使用 serverless 的服务按使用量收费会比较节约成本. 所以推荐学习如下技术栈:

- Data Lake: AWS S3
- Data Catalog: AWS Glue Catalog
- Data Processing: AWS Glue Job
- Data Analytics: AWS Athena



文档
------------------------------------------------------------------------------

- PySpark DataFrame API: https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.html
- PySpark SQL, DataFrame and Datasets Guide: https://spark.apache.org/docs/latest/sql-programming-guide.html
- AWS Glue ETL programming: https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-python.html


AWS Athena 知识点
------------------------------------------------------------------------------


Glue Read Data
------------------------------------------------------------------------------

- 从 S3 读取数据:
    - 使用 `glueContext.create_dynamic_frame.from_options <https://docs.aws.amazon.com/glue/latest/dg/aws-glue-api-crawler-pyspark-extensions-dynamic-frame-reader.html#aws-glue-api-crawler-pyspark-extensions-dynamic-frame-reader-from_options>`_ API 不使用 Catalog, 直接从 S3 读取数据.
    - `from_options <https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-connect.html#aws-glue-programming-etl-connect-s3>`_ API 的详细参数说明. 注意, 不使用
    - 使用 `glueContext.create_dynamic_frame.from_catalog <https://docs.aws.amazon.com/glue/latest/dg/aws-glue-api-crawler-pyspark-extensions-dynamic-frame-reader.html#aws-glue-api-crawler-pyspark-extensions-dynamic-frame-reader-from_catalog>`_, 根据 Glue Catalog 中的定义读取数据.
