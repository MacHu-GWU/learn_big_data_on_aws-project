PySpark examples
==============================================================================


为什么要学 PySpark
------------------------------------------------------------------------------
Spark 是用 Scala 实现的, 原生支持 Scala. 但由于 Python 在数据领域内的流行, 所以 Spark 也支持了 Python, 官方的 Python API 的库叫做 `pyspark <https://pypi.org/project/pyspark/>`_. 其原理是在底层与 Spark 建立一个 session 连接, 所有的计算操作实际发生在服务器上, 并不像 ``pandas`` 一样在本地机器上进行计算. 从性能上来说, 只要不涉及 UDF (User defined function), Python 和 Scala 在几乎没有什么区别.

由于 Python 的数据生态之发达, 第三方库之多, 使得 Spark ETL job 可以用众多的 Python 库和各种云原生系统整合. 而 Scala 的生态就没有这么丰富了, 很可能要自己造很多轮子. 再其次如果不是已经有很多 Scala 经验的开发者, 新学语言的话, 在数据领域你会 Python 还能做很多事, 你会 Scala 可能就只能写 Spark ETL 了.

综上所述, 学习 PySpark 的性价比更好.


怎么学习 PySpark
------------------------------------------------------------------------------
PySpark 的文档分三个部分:

- `Getting Started <https://spark.apache.org/docs/latest/api/python/getting_started/index.html>`_: 快速上手, 感受下 PySpark 是怎么工作的.
- `User Guide <https://spark.apache.org/docs/latest/api/python/user_guide/index.html>`_: 按照 概念, 功能 来组织的文档, 学习 "这一类事情该怎么做".
- `API Reference <https://spark.apache.org/docs/latest/api/python/reference/index.html>`_: 按照 模块, 类, 方法 来组织的文档, 适合用来作为手册查询该怎么做.

链接:

- `文档首页 <https://spark.apache.org/docs/latest/api/python/index.html>`_:
- `Getting Started <https://spark.apache.org/docs/latest/api/python/getting_started/index.html>`_
- `User Guide <https://spark.apache.org/docs/latest/api/python/user_guide/index.html>`_
- `API Reference <https://spark.apache.org/docs/latest/api/python/reference/index.html>`_:
    - Spark SQL: https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql.html
    - Pandas API on Spark: https://spark.apache.org/docs/latest/api/python/reference/pyspark.pandas/index.html
