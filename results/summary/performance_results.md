# Performance Results

| Dataset | Framework | Implementation | Index Terms | Execution Time | Max Memory |
|---|---|---|---:|---:|---:|
| Small dataset | Hadoop | Java MapReduce | 29,240 | 34:31.95 | 223,140 KB |
| Small dataset | Spark | Python PySpark | 29,240 | 0:34.75 | 465,144 KB |

Notes:
- Hadoop and Spark outputs are identical after standardizing tokenization.
- Spark is much faster on this dataset, but uses more memory.
- Hadoop Java implementation uses combiner logic.


| Gutenberg medium | Hadoop | Java MapReduce | 304,951 | 41:00.65 | 226,996 KB |
| Gutenberg medium | Spark | Python PySpark | 304,951 | 6:21.98 | 573,428 KB |

| Gutenberg large 1.7GB | Hadoop | Java MapReduce | 993,379 | 3:39:49 | 251,036 KB |
| Gutenberg large 1.7GB | Spark | Python PySpark | 993,379 | 32:29.60 | 748,116 KB |
