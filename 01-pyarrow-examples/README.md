# PyArrow

在大数据领域, 列式存储格式是用来存储大量数据并提供高性能查询的行业标准. 其中有两种数据格式非常流行 [Apache ORC](https://orc.apache.org/) [Apache Parquet](https://parquet.apache.org/). 其中 Parquet 要更流行一些. [Apache Arrow](https://arrow.apache.org/docs/index.html) 则是一个 in-memory analytics 的数据分析平台, 能把对这些流行的数据格式的 IO, transform 等操作整合起来的一个项目.

在 Python 社区主流的用于数据分析的库是 [Pandas](https://pandas.pydata.org/). **PyArrow** 则是用 Python 操作 Apache Arrow 的一套 API, 同时可以用这套 API 操作 Parquet / ORC 数据格式. 并且提供了一套和 pandas 交互的接口.

在小数据领域 pandas 基本已经够用了, 而在大数据领域, 学习 pyarrow 则是非常有必要的.

本教程是我个人在学习 [pyarrow 官方 API 文档](https://arrow.apache.org/docs/python/index.html) 时的笔记, 以备以后查阅.
