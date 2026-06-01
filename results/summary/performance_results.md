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

| Gutenberg medium | Hadoop | Java MapReduce 2 reducers | 304,951 | 36:08.58 | 221,236 KB |
| Gutenberg medium | Hadoop | Java MapReduce 4 reducers | 304,951 | 36:06.94 | 222,632 KB |

| Gutenberg large 1.7GB | Hadoop | Java MapReduce 2 reducers | 993,379 | 2:57:35 | 374,532 KB |
| Gutenberg large 1.7GB | Hadoop | Java MapReduce 4 reducers | 993,379 | 2:50:40 | 261,504 KB |

## Large Sweep Results

| Dataset | Framework | Implementation | Terms | Time | Max Memory |
|---|---|---|---:|---:|---:|
| Gutenberg large 1.7GB | Hadoop | Java MapReduce 1 reducer | 993,379 | 3:31:00 | 280,892 KB |
| Gutenberg large 1.7GB | Hadoop | Java MapReduce 2 reducers | 993,379 | 3:33:59 | 295,032 KB |
| Gutenberg large 1.7GB | Hadoop | Java MapReduce 4 reducers | 993,379 | 3:38:14 | 283,840 KB |
| Gutenberg large 1.7GB | Hadoop | Java MapReduce 8 reducers | 993,379 | 3:29:24 | 271,488 KB |
| Gutenberg large 1.7GB | Hadoop | Java In-Mapper 1 reducer | 993,379 | 3:21:23 | 278,080 KB |
| Gutenberg large 1.7GB | Hadoop | Java In-Mapper 2 reducers | 993,379 | 3:22:59 | 262,064 KB |
| Gutenberg large 1.7GB | Hadoop | Java In-Mapper 4 reducers | 993,379 | 3:25:12 | 271,360 KB |
| Gutenberg large 1.7GB | Hadoop | Java In-Mapper 8 reducers | 993,379 | 3:16:57 | 263,900 KB |
| Gutenberg large 1.7GB | Spark | PySpark 1 partition | 993,379 | 29:50.75 | 688,044 KB |
| Gutenberg large 1.7GB | Spark | PySpark 2 partitions | 993,379 | 27:36.22 | 762,960 KB |
| Gutenberg large 1.7GB | Spark | PySpark 4 partitions | 993,379 | 28:53.58 | 706,532 KB |
| Gutenberg large 1.7GB | Spark | PySpark 8 partitions | 993,379 | 28:13.16 | 863,868 KB |
