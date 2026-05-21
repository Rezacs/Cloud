# Performance Results

| Dataset | Framework | Implementation | Index Terms | Execution Time | Max Memory |
|---|---|---|---:|---:|---:|
| Small dataset | Hadoop | Java MapReduce | 29,240 | 34:31.95 | 223,140 KB |
| Small dataset | Spark | Python PySpark | 29,240 | 0:34.75 | 465,144 KB |

Notes:
- Hadoop and Spark outputs are identical after standardizing tokenization.
- Spark is much faster on this dataset, but uses more memory.
- Hadoop Java implementation uses combiner logic.

