DuckDB Overview
==============================================================================


What is DuckDB
------------------------------------------------------------------------------
`DuckDB <https://duckdb.org/>`_ 官网称自己为 in-process SQL OLAP database management system. **你也可以将它理解为 OLAP 届的 Sqlite**. Sqlite 是一个无服务器, 嵌入式, 单文件的关系数据库. **DuckDB 则也是一个无服务器, 嵌入式, 单文件的大数据分析数据库**.

DuckDB 的牛逼之处在于, 它支持从 文件 (本地的或网络上的), Sqlite, Postgres 读取数据, 而这里的读取和很多数据分析引擎需要将文件先读到内存中, 然后再进行查询的方式不同, 它可以利用数据所在的系统本身的特性, 无需将数据读到内存中便可直接进行分析. 举例来说, 你的 S3 上有很多 partition 目录结构, 每个目录下有很多 parquet 数据文件, DuckDB 则无需你创建 Catalog Table, 就可以利用这些 partition 以及 parquet 文件的元数据进行 push down predicate, 从而大幅提高性能. 这听起来像是 Presto 和 AWS Athena 做的事情, 但是 DuckDB 可是一个无需部署服务器, 单节点就能利用多核 CPU, 可以嵌入到编程语言中的包. 使用 DuckDB 的硬件成本远远低于 Presto 和 Athena.

再举一个例子, 如果你用业界已经是最高性能的数据表分析库 polars 来分析 S3 上的数据, 你首先要用 S3 的目录结构, 进行手动的 if else 根据 partition 来分析要从哪些 S3 object 中读数据. 其次, 如果你不花很多时间优化, 你需要将这些数据全部读到内存中, 然后用 polars 进行筛选分析. 而如果你要利用 row group 的元数据来跳过 row group, 或是只读取计算所需的 column, 你可以想象一下你有多少代码需要写, 并且这些代码的扩展性几乎为 0. 而在 DuckDB 中你只要写 SQL 即可.

根据我的分析, **作为 DuckDB 的用户**, 它能提供和业内 MVCC 数据分析框架同一级别的性能, 但却只需要一个廉价的单机. **作为 DuckDB 背后的商业公司**, 它可以被封装成像 Athena, Redshift 那样的数据仓库, 并且作为一个 Serverless 的服务给用户使用, 但是硬件成本却远远低于这些服务. 并且由于它的嵌入式本质, 存储就是一个文件, 而计算就是一个进程, 这使得它的 scale in/out 的速度完全可以达到秒级. 我认为 DuckDB 的想象空间是巨大的, 会有非常多有价值的产品形态. 目前这个 DuckDB 背后的商业公司 MotherDuck 2022 年 11 月 融资了 35M, 公司有 11-50 个人, 按照 30 个人, 一人年薪 10w 美元算, 大约能烧一年半到 2024 年 5 月. 目前 MotherDuck 已经提供了一个云上的界面可供用户登录后直接用云上的资源对数据进行分析. 以及一个 Python API, 可以在任何 Python 脚本中嵌入一段用 DuckDB 进行分析的程序, 其本质是调用远程的 MotherDuck 云上的资源进行分析, 然后将结果传给客户端.

总而言之, DuckDB 是一个很新颖的软件, 作为开发者它是一个非常强大且好用的工具, 学习成本极低, 非常推荐学习. 而作为企业用户, 可以将所需的计算资源和要被扫描的数据比较容易预估, 使用频率高的分析任务从昂贵的 AWS Athena 或是 AWS Glue 上剥离. 也是非常有价值的.


DuckDB Use Case
------------------------------------------------------------------------------
**本地数据文件分析**

如果你是要对文件进行一些筛选, 聚合, 排序, 列式分析, 甚至用自定义的 UDF 来做行分析, DuckDB 既可以讲数据 load 到 DB 中持久化然后分析, 也可以不进行 load 并直接进行分析.

**对传统数据进行 Full Text Search 分析**

通常我们必须使用 ElasticSearch 才能进行 Full text search, 而 Sqlite, Postgres 的 Full Text Search 功能都需要你将数据导入到数据库中, 如果要用 Postgres 你可能还要下载 Container. 而用 DuckDB 可以用简单的几行代码就直接进行 Full Text Search 分析.

**对云上的 DataLake 进行分析**

DataLake 中的数据往往远远超过单机内存. 而我们最终分析用到的筛选后的数据往往是通过 Partition, Range Filter 筛选后的, 而且我们可能只选择我们需要的列, 所以最终参与计算的数据很可能并不大. 有了 DuckDB, 我们可以无需创建 Glue Catalog, 也无需使用 Athena (Athena 主要是给人用的, 它有并发的 quota, 一般不是给机器用的), 或是 Glue + PySpark 就可以直接进行分析了. 这使得我们可以用 Lambda 或是容器来运行数据分析.


Link
------------------------------------------------------------------------------
- `DuckDB 官网 <https://duckdb.org/>`_
- `DuckDB 文档 <https://duckdb.org/docs/archive/0.8.1/>`_
- `MotherDuck 官网 <https://motherduck.com/>`_
- `MotherDuck crunchbase 融资情况 <https://www.crunchbase.com/organization/motherduck>`_
