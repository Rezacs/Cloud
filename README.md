# Inverted Index and Search

**Final Cloud Computing Project — A.Y. 2024/2025**

## Goal

This project builds an **inverted index** from a collection of text files. For each word, the output reports the files where the word appears and the number of occurrences in each file.

**Example output:**

```
cloud    doc1.txt:2 doc2.txt:1
hadoop   doc2.txt:3
```

---

## Implementations

| Implementation                  | Language |
| ------------------------------- | -------- |
| Hadoop MapReduce                | Java     |
| Hadoop with In-Mapper Combining | Java     |
| Hadoop with Multiple Reducers   | Java     |
| Spark                           | Python   |
| Sequential baseline             | Python   |

---

## Cluster

Fully distributed Hadoop cluster:

| Role       | IP Address |
| ---------- | ---------- |
| NameNode   | 10.1.1.166 |
| DataNode-2 | 10.1.1.213 |
| DataNode-3 | 10.1.1.163 |

---

## Project Structure

```
hadoop-java/          Hadoop Java implementation
spark-python/         Spark Python implementation
search/               Non-parallel query search script
sequential-python/    Sequential Python baseline
scripts/              Run scripts
results/summary/      Experiment summaries
```

---

## Build

### Hadoop Java

```bash
cd hadoop-java
mvn clean package
```

**JAR output:**

```
hadoop-java/target/hadoop-inverted-index-1.0.jar
```

---

## Run

### Hadoop Java (Standard)

```bash
hadoop jar hadoop-java/target/hadoop-inverted-index-1.0.jar \
  it.unipi.cloud.InvertedIndex \
  /input/gutenberg-medium \
  /output/hadoop-index-gutenberg-medium
```

### Hadoop Java (Custom Reducers)

```bash
hadoop jar hadoop-java/target/hadoop-inverted-index-1.0.jar \
  it.unipi.cloud.InvertedIndex \
  /input/gutenberg-medium \
  /output/hadoop-index-gutenberg-medium-r4 \
  4
```

### Hadoop In-Mapper Combining

```bash
hadoop jar hadoop-java/target/hadoop-inverted-index-1.0.jar \
  it.unipi.cloud.InvertedIndexInMapper \
  /input/gutenberg-medium \
  /output/hadoop-index-gutenberg-medium-inmapper
```

### Spark Python

```bash
spark-submit spark-python/inverted_index_spark.py \
  hdfs:///input/gutenberg-medium \
  hdfs:///output/spark-index-gutenberg-medium
```

### Sequential Python

```bash
python3 sequential-python/inverted_index_sequential.py \
  /tmp/gutenberg-medium-clean \
  results/sequential-medium-clean/index.txt
```

### Search Query

```bash
python3 search/search_index.py results/spark-small/index.txt "cloud computing"
```

> The output contains only filenames.

---

## Datasets

| Size   | Description                      |
| ------ | -------------------------------- |
| Small  | Text classification dataset      |
| Medium | Project Gutenberg subset         |
| Large  | Project Gutenberg ~1.7 GB subset |

---

## Results

Performance results are saved in:

```
results/summary/performance_results.csv
results/summary/performance_results.md
```

---

## Notes

- Large datasets and full output indexes are **not committed** to GitHub.
- Only source code and summary results are tracked.
