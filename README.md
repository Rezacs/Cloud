# Inverted Index and Search — Cloud Computing Project
**Academic Year 2024/2025**  
Reza Almassi · Ronald Omoding · Rojan Shrestha  
University of Pisa — Cloud Computing Course

---

## Overview

This project implements and evaluates an **inverted index** builder — the foundational data structure behind search engines like Google. For each word found across a collection of text files, the index records every filename where the word appears and how many times:

```
cloud    doc1.txt:3 doc2.txt:1
hadoop   doc2.txt:2 doc3.txt:5
```

Five implementations are provided and benchmarked across 25 datasets (0.43 MB to 3.84 GB):

| Method | Language | Engine |
|--------|----------|--------|
| Hadoop Base | Java | Hadoop MapReduce (with Combiner) |
| Hadoop InMapper | Java | Hadoop MapReduce (In-Mapper Combining) |
| PySpark | Python | Apache Spark |
| Java Spark | Java | Apache Spark |
| Sequential | Python | Single-machine baseline |

---

## Project Structure

```
Cloud/
├── hadoop-java/                        # Hadoop MapReduce implementation
│   ├── src/main/java/it/unipi/cloud/
│   │   ├── InvertedIndex.java          # Hadoop Base (combiner)
│   │   ├── InvertedIndexInMapper.java  # Hadoop InMapper combining
│   │   ├── StopWords.java              # Stop-word loader utility
│   │   └── WholeFileInputFormat.java   # Custom input format (one file = one record)
│   ├── src/main/resources/
│   │   └── stopwords.txt               # Stop-word list
│   └── pom.xml
│
├── spark-java/                         # Java Spark implementation
│   ├── src/main/java/it/unipi/cloud/
│   │   └── JavaSparkInvertedIndex.java # Java Spark (combineByKey, no global sort)
│   └── pom.xml
│
├── spark-python/                       # PySpark implementation
│   └── spark_inverted_index_fastest.py # PySpark (combineByKey, no global sort)
│
├── sequential-python/                  # Single-machine baseline
│   └── inverted_index_sequential.py
│
├── search/                             # Search query system
│   └── search_index.py                 # Single/multi-term search on generated index
│
├── report/                             # Project report (LaTeX)
│
├── results/                            # Experiment results and summary archives
│   └── analysis/
│       └── summary_light_archives/
│           ├── final_all_experiments_v6.csv   # Main results CSV (625 runs)
│           ├── anomalies_v6.log
│           └── final_exp_*_v6_summary_light.tar.gz  # Per-dataset logs + monitors
│
├── run_all_v6.sh                       # Main experiment runner (all 25 datasets)
├── extract_tarball_metrics.sh          # Post-run metrics extractor
└── INFO.txt
```

---

## Cluster Setup

Three-node fully distributed Hadoop cluster:

| Node | Role | IP |
|------|------|----|
| `namenode` | NameNode, ResourceManager, DataNode, NodeManager | 10.1.1.166 |
| `datanode2` | DataNode, NodeManager | 10.1.1.213 |
| `datanode3` | DataNode, NodeManager | 10.1.1.163 |

**Software versions:** Hadoop 3.1.3 · Java 8 · Apache Spark  
**YARN:** 5,400 MB / 3 vcores per node → 16,200 MB total  
**HDFS replication factor:** 2

---

## Prerequisites

- Hadoop 3.1.3 installed at `/opt/hadoop`
- Apache Spark installed at `/usr/local/spark`
- Java 8 (`/usr/lib/jvm/java-8-openjdk-amd64`)
- Python 3 with PySpark
- Maven (`mvn`) for building JARs
- SSH passwordless access from `namenode` to `datanode2` and `datanode3`
- All HDFS/YARN daemons running (`start-dfs.sh`, `start-yarn.sh`)

---

## Building the JARs

```bash
# Hadoop MapReduce JAR
cd ~/Cloud/hadoop-java
mvn clean package

# Java Spark JAR
cd ~/Cloud/spark-java
mvn clean package
```

Output JARs:
- `hadoop-java/target/hadoop-inverted-index-1.0.jar`
- `spark-java/target/spark-java-inverted-index-1.0.jar`

---

## Running a Single Job

### Hadoop Base
```bash
hadoop jar hadoop-java/target/hadoop-inverted-index-1.0.jar \
  it.unipi.cloud.InvertedIndex \
  /input/ds-08-1p1gb-6680files \
  /output/test-hadoop-base \
  8 \
  /stopwords.txt
```

### Hadoop InMapper
```bash
hadoop jar hadoop-java/target/hadoop-inverted-index-1.0.jar \
  it.unipi.cloud.InvertedIndexInMapper \
  /input/ds-08-1p1gb-6680files \
  /output/test-inmapper \
  8 \
  /stopwords.txt
```
`8` = number of reducers (`r`).

### PySpark
```bash
spark-submit \
  --master yarn --deploy-mode client \
  --num-executors 3 --executor-memory 3584m --executor-cores 2 \
  --driver-memory 2g \
  --conf spark.executor.memoryOverhead=512 \
  --conf spark.dynamicAllocation.enabled=false \
  spark-python/spark_inverted_index_fastest.py \
  /input/ds-08-1p1gb-6680files \
  hdfs://namenode:9000/output/test-pyspark \
  16 \
  hadoop-java/src/main/resources/stopwords.txt
```

### Java Spark
```bash
spark-submit \
  --master yarn --deploy-mode client \
  --num-executors 3 --executor-memory 3584m --executor-cores 2 \
  --driver-memory 2g \
  --conf spark.executor.memoryOverhead=512 \
  --conf spark.dynamicAllocation.enabled=false \
  --class it.unipi.cloud.JavaSparkInvertedIndex \
  spark-java/target/spark-java-inverted-index-1.0.jar \
  /input/ds-08-1p1gb-6680files \
  hdfs://namenode:9000/output/test-java-spark \
  16 \
  hadoop-java/src/main/resources/stopwords.txt
```
`16` = number of partitions (`p`).

### Sequential Python
```bash
# Copy input from HDFS to local first
hdfs dfs -get /input/ds-08-1p1gb-6680files /tmp/local_input/
python3 sequential-python/inverted_index_sequential.py \
  /tmp/local_input/ \
  /tmp/output_sequential.txt
```

---

## Search Query System

```bash
# Single-term query
python3 search/search_index.py /tmp/output_sequential.txt "cloud"

# Multi-term query (returns files containing ALL terms)
python3 search/search_index.py /tmp/output_sequential.txt "cloud computing"
```

Output: filenames only, one per line, no occurrence counts.

---

## Running All Experiments

The main experiment script runs all 5 methods across all 25 datasets with the full parameter sweep automatically:

```bash
# Make executable
chmod +x run_all_v6.sh

# Run in a tmux session (takes several hours)
tmux new -s experiments
./run_all_v6.sh
```

**What it does:**
- Builds both JARs via Maven
- Uploads stop-words to HDFS
- Runs Hadoop Base and InMapper with `r ∈ {1, 2, 4, 8, 16, 24}`
- Runs PySpark and Java Spark with `p ∈ {4, 8, 16, 24, 32, 40}`
- Runs Sequential Python locally
- Monitors YARN memory (5-second samples) during every job
- Cleans HDFS output and `/tmp` on all 3 nodes between every job
- Saves per-dataset summary tarballs
- Writes all results to `results/analysis/summary_light_archives/final_all_experiments_v6.csv`
- Flags anomalies (non-zero exit or zero output lines) to `anomalies_v6.log`

**Estimated runtime:** 8–12 hours for all 25 datasets.

---

## Extracting Additional Metrics from Tarballs

After the main run, extract GC time, CPU efficiency, RAM ramp-up, and active resource duration from the per-dataset summary archives:

```bash
chmod +x extract_tarball_metrics.sh
./extract_tarball_metrics.sh
```

Produces 5 CSV files in `results/analysis/summary_light_archives/`:
- `metrics_gc_time.csv`
- `metrics_cpu_efficiency.csv`
- `metrics_rampup.csv`
- `metrics_active_duration.csv`
- `metrics_ram_gap.csv`

---

## Datasets

13 base datasets (ds-00 through ds-12) were prepared from three source collections: Project Gutenberg books, Internet Archive texts, and Kaggle news articles. 12 additional combined datasets (combo-01 through combo-12) were created by passing multiple HDFS paths to the jobs — no physical data duplication.

| Range | Datasets | Size |
|-------|----------|------|
| Tiny | ds-00, ds-01, combo-06 | 0.43 MB – 4.57 MB |
| Small | ds-11, ds-02, ds-03, ds-04, ds-05 | 96 MB – 500 MB |
| Medium | ds-06, ds-07, ds-08, ds-12 + combos | 559 MB – 1.6 GB |
| Large | ds-09, ds-10 + combos | 1.1 GB – 3.84 GB |

Full dataset table: see `report/` or Table 2 in the project report.

---

## Key Results

All 625 job runs completed (624 successful; 1 sequential Python OOM at 3.14 GB).

| Scale | Fastest method | Note |
|-------|---------------|------|
| < 100 MB | Sequential Python | Zero distributed overhead at tiny scale |
| 100 MB – ~1 GB | Hadoop InMapper | In-mapper combining beats Spark startup overhead |
| > 1 GB | Java Spark | In-memory shuffle overtakes disk-backed Hadoop |

**InMapper speedup over Hadoop Base:** 1.06× – 1.83× (mean 1.53×) across all 25 datasets.  
**Spark crossover:** Java Spark beats InMapper from ~260 MB onward on most datasets; consistently faster above 1 GB.  
**Best result on largest dataset (3.84 GB):** Java Spark 245 s · PySpark 265 s · InMapper 345 s · Hadoop Base 498 s · Sequential 805 s.

---

## Output Format

```
word<TAB>filename1:count1 filename2:count2 ...
```

Example:
```
cloud    doc1.txt:3 doc2.txt:1
hadoop   doc2.txt:2 doc3.txt:5
index    doc1.txt:1 doc2.txt:1 doc3.txt:2
```

Output correctness was validated by comparing line counts across all methods on every dataset.

---

## Notes

- The sequential Python solution reads from **local filesystem**, not HDFS — this is intentional; it serves as a single-machine baseline.
- For combined datasets, Hadoop and Spark accept comma-separated HDFS input paths natively (e.g. `/input/ds-07,..,/input/ds-12`).
- The `Final_Results/` directory contains only summary tarballs and CSVs, not raw HDFS output (deleted after each job to save space).
