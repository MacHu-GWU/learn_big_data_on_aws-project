Process Super Big JSON File
==============================================================================
有的时候我们需要处理体积巨大的 JSON 文件. 如果是 multi line JSON 文件, 那事情就比较简单了, 你可以轻易的遍历所有行, 只是花点时间而已. 而如果是一个巨大的 Object, 一个 array 最终是 nest 在很深的地方, 并且整个 JSON 仅仅只有 1 行, 这就比较难办了.

**主要思路**

一般一个 JSON 之所以大, 通常是有某一个 Array 里面有非常多的 item. 或者一个 Dictionary 里面有很多很多的 Key (1, 2, 3, ...). 所以我们只要能将这些 Item 分拆出来成为小文件, 然后把剩下的数据写入到另外一个文件.

**工具**

ijson 是一个基于 C 的 Python 库, 能顺序的读入字节流来解析 JSON, 这样能节约非常多的内存. 基于 ijson 库, 我们能定位到有很多 item 的 Array 节点, 然后将这些数据拆分出来. 换言之, 我们用额外的时间节约了内存空间.

下面是两个脚本, 分别是处理本地文件, 和处理在 S3 上的文件的版本.

.. literalinclude:: split_json_locally.py
   :language: python
   :linenos:

.. literalinclude:: lambda_function.py
   :language: python
   :linenos:
