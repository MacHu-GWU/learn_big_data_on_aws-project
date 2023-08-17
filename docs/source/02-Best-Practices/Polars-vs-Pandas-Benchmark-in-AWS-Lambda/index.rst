Polars vs Pandas Benchmark in AWS Lambda
==============================================================================
Keywords: Polars, Pandas, AWS, Lambda, Glue, ETL, Demo


Overview
------------------------------------------------------------------------------
`Polars <https://www.pola.rs/>`_ is a **lightning-fast dataframe library** built on top of Rust. It also has Python SDK that using the compiled rust code under the hood. It has the following advantage over `pandas <https://pandas.pydata.org/>`_:

1. Due to the underlying arrow2 implementation, it is multi-thread friendly that uses multiple CPU cores by default. **It is usually 2-5x faster than pandas when reading columnar data format (parquet), 8-10x faster than pandas when reading row oriented dataformat (CSV, JSON)**.
2. The memory data model in polars is more compact and efficient. As a result, the memory consumption of polars Dataframe usually is as same as the raw data (sometime even less due to the compression). Comparing to pandas, it usually uses memory that is double size of the raw data (when you have string).
3. Most column-oriented transformation uses vectorized operation and multiple CPU core out-of-the-box, which results in 4-8 times faster than pandas.
4. Polars use lazy load and zero-copy technique heavily. Just like transformation and action concepts in Spark, it only execute the transformation when necessary so that there's no need to copy the data for intermediate step. Comparing to pandas, it usually use 1/4 ~ 1/10 memory than pandas for transformation that depends on the number of intermediate steps.

`AWS Glue <https://aws.amazon.com/glue/>`_ is a service that allows developer to run ETL job on spark without provisioning any infrastructure. However, the AWS Glue development experience is not very good. By default, the development environment is not interactive, you usually has to wait at least 2-5 minutes to execute even just one line of code. Glue has an option to use Jupyter Notebook for development. However, it requires some setup and the Glue Job runtime and Jupyter Notebook runtime are not exactly the same, so it may cause unexpected behavior in production. The last challenge would be the deployment and DevOps. Most modern application support immutable deployment, version control, blue green deployment, and canary deployment. However, AWS Glue ETL job doesn't support any of these out of the box.

`AWS Lambda <https://aws.amazon.com/pm/lambda/>`_ is a service that allow developer to run code in a container runtime without provisioning any infrastructure, and also no need to setup the programming language runtime. However, it has a hard limit that can only have 10GB memory and 15 minutes execution time.

Usually, AWS Lambda is not a good option for Big Data ETL Process. However, based on my career experience, most of the necessary data in an ETL job is less than 1M rows. For example, your data lake may have 1B rows of data, but in your ETL job, you may only need 1M rows for calculation.

**In this document, I would like to explore the possibility to use AWS Lambda + polars Python library to perform medium size dataset ETL job.**.


Experimental Design
------------------------------------------------------------------------------
Data Schema:

- Has 25 columns.
- 5 columns' data type is int64, values are between 1 and 100000, example: 397647.
- 5 columns' data type is float, values are between 0, 1, example: 0.44934012731611805.
- 5 columns' data type is short string, values are uuid string, example: ``daa03354-c777-4b4a-b649-30998f7bd9e3``.
- 5 columns' data type is long string, values are lorem ipsum text has 3 ~ 6 sentences, example: ``Picture wait add environment PM weight music. Type tax chair friend. Data might read value three involve.``.
- 5 columns' data type is timestamp, values are from random datetime in microseconds from 2000-01-01 to 2023-01-01, example: ``2008-11-08T14:37:77.638096Z``.

Data Files:

- We create 100 files.
- Each file has 100,000 rows.
- Data file format is parquet with snappy compression.
- Each file is about 60MB with snappy compressed. 85MB if uncompressed.

Lambda Function:

- Python3.9
- Memory: 10238 MB (cap is 10GB)
- Architecture: x84_64

Python Libraries (release time near 2023-01-01):

- polars == 1.7.15
- pandas == 1.5.3, pyarrow == 9.0.0

We try to read as many file as we could. If we can fit 10M rows in memory, usually we can handle 1M (1/10) rows' dataset. It is because we may have to create copy of the data during data transformation.


Experiment Result
------------------------------------------------------------------------------
Result sheet description (All the measurement is the average of 10 lambda invocation):

- engine: polars or pandas
- n_files: how many files we read
- n_rows: how many rows we read (we can handle 1/10 of this number in production)
- raw size: the total raw parquet file size (no compression)
- polars_time: how long it take to read all files with polars
- pandas_time: how long it take to read all files with pandas
- polars_mem: polars memory usage
- pandas_mem: pandas memory usage

+---------+------------+---------------+-------------------+-------------------+-----------------+-----------------+
| n_files |   n_rows   | raw_size (MB) | polars_time (sec) | pandas_time (sec) | polars_mem (MB) | pandas_mem (MB) |
+=========+============+===============+===================+===================+=================+=================+
|    1    |   100,000  |       85      |        0.8        |        1.3        |       325       |       600       |
+---------+------------+---------------+-------------------+-------------------+-----------------+-----------------+
|    10   |  1,000,000 |      850      |         8         |         13        |      1,200      |      1,900      |
+---------+------------+---------------+-------------------+-------------------+-----------------+-----------------+
|    20   |  2,000,000 |     1,700     |         16        |         25        |      2,200      |      3,450      |
+---------+------------+---------------+-------------------+-------------------+-----------------+-----------------+
|    30   |  3,000,000 |     2,550     |         23        |         40        |      3,050      |      4,900      |
+---------+------------+---------------+-------------------+-------------------+-----------------+-----------------+
|    40   |  4,000,000 |     3,400     |         32        |         51        |      4,000      |      6,400      |
+---------+------------+---------------+-------------------+-------------------+-----------------+-----------------+
|    50   |  5,000,000 |     4,250     |         42        |         63        |      5,000      |      7,900      |
+---------+------------+---------------+-------------------+-------------------+-----------------+-----------------+
|    60   |  6,000,000 |     5,100     |         53        |         85        |      5,950      |      9,400      |
+---------+------------+---------------+-------------------+-------------------+-----------------+-----------------+
|    70   |  7,000,000 |     5,950     |        ...        |        OOM        |       ...       |       OOM       |
+---------+------------+---------------+-------------------+-------------------+-----------------+-----------------+
|    80   |  8,000,000 |     6,800     |        ...        |        OOM        |       ...       |       OOM       |
+---------+------------+---------------+-------------------+-------------------+-----------------+-----------------+
|    90   |  9,000,000 |     7,650     |        ...        |        OOM        |       ...       |       OOM       |
+---------+------------+---------------+-------------------+-------------------+-----------------+-----------------+
|   100   | 10,000,000 |     8,500     |        110        |        OOM        |      9,500      |       OOM       |
+---------+------------+---------------+-------------------+-------------------+-----------------+-----------------+


Conclusion
------------------------------------------------------------------------------
- Due to the compact in memory data structure implementation, to dataframe size is similar to the raw data size in polars, but pandas spend 2X more than the raw data size.
- Including the S3 file IO times, polars read parquet file 1.5 ~ 2X faster than pandas if dataset has lots of string. In the official benchmark, polars is 8-10X faster than pandas when reading a CSV / JSON.
- With Polars, we are able to process 1M rows dataset in Lambda Function. Potentially more because polars use lazy load and zero copy technique to reduce memory usage, it less likely you really need to create copy the data.
- With Pandas, we are able to process 650K rows dataset in Lambda Function.
- If your ETL job source dataset is less than 1M rows (decrease this number if your average size of the row is larger than this experiment, vice versa), and your ETL job doesn't requires special write engine like Delta Lake, Hudi, IceBerg, you can consider using Lambda + Polars to do ETL job that originally been down in AWS Glue. And you get these goodies:
    - better development experience in Lambda
    - easy to test and you can fully test your code in unit test
    - better deployment strategy (versioned deployment, blue/green, canary, out-of-the-box)
    - easy to orchestrate
    - easy to integrate with other service


Additional Thought
------------------------------------------------------------------------------
- If your input data is not a list of files, it is actually a result of a SQL query, you can use AWS Athena to run the query (there is a 200 concurrent limit), and load the result into Lambda Function.


Code Example
------------------------------------------------------------------------------
.. literalinclude:: ./create_data.py
   :language: python
   :linenos:

.. literalinclude:: ./lambda_function_polars.py
   :language: python
   :linenos:

.. literalinclude:: ./lambda_function_pandas.py
   :language: python
   :linenos:


Demo
------------------------------------------------------------------------------
.. raw:: html
    :file: ./Lambda-Plus-Polars-Another-Powertool-for-ETL.drawio.html
