#!/usr/bin/env bash
set -u

export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export HADOOP_HOME=/opt/hadoop
export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
export YARN_CONF_DIR=/opt/hadoop/etc/hadoop
export SPARK_HOME=/usr/local/spark
export PATH=$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH

cd ~/Cloud

REDUCERS="1 2 4 8 16 24"
NODES="namenode datanode2 datanode3"

JAR="$HOME/Cloud/hadoop-java/target/hadoop-inverted-index-1.0.jar"
SPARK_SCRIPT="$HOME/Cloud/spark-python/spark_inverted_index_fastest.py"
JAVA_SPARK_JAR="$HOME/Cloud/spark-java/target/spark-java-inverted-index-1.0.jar"
SEQ_SCRIPT="$HOME/Cloud/sequential-python/inverted_index_sequential.py"

STOPWORDS_HDFS="/stopwords.txt"
STOPWORDS_LOCAL="$HOME/Cloud/hadoop-java/src/main/resources/stopwords.txt"

GLOBAL_CSV="results/analysis/final_all_experiments_summary.csv"
mkdir -p results/analysis
echo "dataset,method,param,exit_status,elapsed_seconds,wall_time,max_process_rss_kb,max_cluster_ram_used_gb,lines" > "$GLOBAL_CSV"

echo "=== CREATE / UPDATE FASTEST PYSPARK SCRIPT ==="
mkdir -p spark-python
cat > "$SPARK_SCRIPT" <<'PY'
import os
import re
import sys
from pyspark import SparkConf, SparkContext

TOKEN_RE = re.compile(r"[a-z0-9]+")

def load_stopwords(sc, path):
    if not path:
        return set()
    try:
        return {
            line.strip().lower()
            for line in sc.textFile(path).collect()
            if line.strip() and not line.strip().startswith("#")
        }
    except Exception:
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                return {
                    line.strip().lower()
                    for line in f
                    if line.strip() and not line.strip().startswith("#")
                }
        return set()

def parse_args():
    if len(sys.argv) < 3 or len(sys.argv) > 5:
        print("Usage: spark_inverted_index_fastest.py <input> <output> [numPartitions] [stopwordsPath]")
        sys.exit(1)

    input_path = sys.argv[1]
    output_path = sys.argv[2]
    num_partitions = 4
    stopwords_path = None

    if len(sys.argv) >= 4:
        try:
            num_partitions = int(sys.argv[3])
        except ValueError:
            stopwords_path = sys.argv[3]

    if len(sys.argv) == 5:
        stopwords_path = sys.argv[4]

    return input_path, output_path, num_partitions, stopwords_path

def main():
    input_path, output_path, num_partitions, stopwords_path = parse_args()

    conf = (
        SparkConf()
        .setAppName("PySpark Inverted Index Fastest No Sort")
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .set("spark.shuffle.compress", "true")
        .set("spark.shuffle.spill.compress", "true")
        .set("spark.rdd.compress", "true")
        .set("spark.default.parallelism", str(num_partitions))
        .set("spark.sql.shuffle.partitions", str(num_partitions))
        .set("spark.python.worker.reuse", "true")
    )

    sc = SparkContext(conf=conf)
    stopwords_bc = sc.broadcast(load_stopwords(sc, stopwords_path))

    files = sc.wholeTextFiles(input_path, minPartitions=num_partitions)

    def file_to_postings(file_content):
        path, text = file_content
        filename = path.rsplit("/", 1)[-1]
        sw = stopwords_bc.value

        counts = {}
        text = text.lower()

        for match in TOKEN_RE.finditer(text):
            word = match.group(0)
            if word not in sw:
                counts[word] = counts.get(word, 0) + 1

        return [(word, f"{filename}:{count}") for word, count in counts.items()]

    def create_combiner(v):
        return v

    def merge_value(acc, v):
        return f"{acc} {v}"

    def merge_combiners(a, b):
        return f"{a} {b}"

    inverted_index = (
        files
        .flatMap(file_to_postings)
        .combineByKey(
            create_combiner,
            merge_value,
            merge_combiners,
            numPartitions=num_partitions
        )
    )

    inverted_index.map(lambda x: f"{x[0]} {x[1]}").saveAsTextFile(output_path)

    stopwords_bc.destroy()
    sc.stop()

if __name__ == "__main__":
    main()
PY

run_cmd_with_monitor () {
  dataset="$1"
  method="$2"
  param="$3"
  log="$4"
  mon="$5"
  shift 5

  echo "=== $dataset | $method | $param ==="
  echo "timestamp,node,total_mb,used_mb,free_mb,available_mb" > "$mon"

  (
    while true; do
      ts=$(date "+%Y-%m-%d %H:%M:%S")
      for node in $NODES; do
        ssh hadoop@$node "free -m | awk '/^Mem:/ {print \$2\",\" \$3\",\" \$4\",\" \$7}'" 2>/dev/null \
          | awk -v ts="$ts" -v node="$node" '{print ts "," node "," $0}'
      done
      sleep 5
    done
  ) >> "$mon" &
  monpid=$!

  start=$(date +%s)

  timeout 7200 "$@" > "$log" 2>&1
  code=$?

  end=$(date +%s)
  sec=$((end - start))

  kill "$monpid" 2>/dev/null || true
  wait "$monpid" 2>/dev/null || true

  echo "Elapsed seconds: $sec" >> "$log"
  echo "Finished $dataset | $method | $param | exit=$code | seconds=${sec}s"

  return "$code"
}

parse_wall () {
  grep -E "Elapsed \(wall clock\)" "$1" | tail -1 | awk -F': ' '{print $2}'
}

parse_rss () {
  grep -E "Maximum resident" "$1" | tail -1 | awk '{print $6}'
}

cluster_max_gb () {
  awk -F, '
    NR > 1 {
      used[$1] += $4
    }
    END {
      max = 0
      for (t in used) if (used[t] > max) max = used[t]
      printf "%.2f", max / 1024
    }
  ' "$1"
}

count_lines () {
  hdfs dfs -cat "$1/part-*" 2>/dev/null | wc -l
}

make_summary () {
  DATASET="$1"

  OUT_BASE="/output/final-exp-$DATASET/$DATASET"
  LOG_DIR="results/logs/final_exp_$DATASET"
  ANALYSIS_DIR="results/analysis/final_exp_$DATASET"
  MONITOR_DIR="results/monitor/final_exp_$DATASET"
  SUMMARY_DIR="results/analysis/final_exp_${DATASET}_summary"
  TAR_FILE="results/analysis/final_exp_${DATASET}_summary_light.tar.gz"
  SEQ_OUT="$ANALYSIS_DIR/sequential_outputs/index_${DATASET}.txt"

  echo "=== CREATE SUMMARY $DATASET ==="

  rm -rf "$SUMMARY_DIR"
  rm -f "$TAR_FILE"
  mkdir -p "$SUMMARY_DIR/logs" "$SUMMARY_DIR/monitor" "$SUMMARY_DIR/samples"

  cp "$LOG_DIR"/*.log "$SUMMARY_DIR/logs/" 2>/dev/null || true
  cp "$MONITOR_DIR"/*.csv "$SUMMARY_DIR/monitor/" 2>/dev/null || true
  cp "$ANALYSIS_DIR/dataset_info.txt" "$SUMMARY_DIR/" 2>/dev/null || true

  {
    echo "job,max_cluster_ram_used_gb"
    for csv in "$SUMMARY_DIR"/monitor/*.csv; do
      [ -f "$csv" ] || continue
      job=$(basename "$csv" .csv)
      echo "$job,$(cluster_max_gb "$csv")"
    done
  } > "$SUMMARY_DIR/cluster_memory_summary.csv"

  rm -f "$SUMMARY_DIR/line_counts.txt"

  for r in $REDUCERS; do
    for method in hadoop-base hadoop-inmapper; do
      path="$OUT_BASE/$method-r$r"
      echo -n "$path: " >> "$SUMMARY_DIR/line_counts.txt"
      count_lines "$path" >> "$SUMMARY_DIR/line_counts.txt"
    done
  done

  for p in $REDUCERS; do
    for method in pyspark-fastest java-spark; do
      path="$OUT_BASE/$method-p$p"
      echo -n "$path: " >> "$SUMMARY_DIR/line_counts.txt"
      count_lines "$path" >> "$SUMMARY_DIR/line_counts.txt"
    done
  done

  echo -n "sequential-$DATASET-local: " >> "$SUMMARY_DIR/line_counts.txt"
  wc -l < "$SEQ_OUT" 2>/dev/null >> "$SUMMARY_DIR/line_counts.txt" || echo "0" >> "$SUMMARY_DIR/line_counts.txt"

  hdfs dfs -du -s -h "$OUT_BASE"/* > "$SUMMARY_DIR/output_sizes.txt" 2>/dev/null || true
  du -h "$SEQ_OUT" >> "$SUMMARY_DIR/output_sizes.txt" 2>/dev/null || true

  {
    for log in "$SUMMARY_DIR/logs"/*.log; do
      [ -f "$log" ] || continue
      echo "--- $(basename "$log") ---"
      grep -E "Elapsed|Elapsed seconds|Maximum resident|User time|System time|Percent of CPU|Launched map tasks|Launched reduce tasks|Map input records|Map output records|Reduce input records|Reduce output records|Job Finished|Job failed|Exception|ERROR|Exit status" "$log"
    done
  } > "$SUMMARY_DIR/performance_summary.txt"

  hdfs dfs -cat "$OUT_BASE/hadoop-base-r1/part-*" 2>/dev/null | head -20 > "$SUMMARY_DIR/samples/${DATASET}_hadoop_base_sample.txt"
  hdfs dfs -cat "$OUT_BASE/hadoop-inmapper-r1/part-*" 2>/dev/null | head -20 > "$SUMMARY_DIR/samples/${DATASET}_hadoop_inmapper_sample.txt"
  hdfs dfs -cat "$OUT_BASE/pyspark-fastest-p1/part-*" 2>/dev/null | head -20 > "$SUMMARY_DIR/samples/${DATASET}_pyspark_fastest_sample.txt"
  hdfs dfs -cat "$OUT_BASE/java-spark-p1/part-*" 2>/dev/null | head -20 > "$SUMMARY_DIR/samples/${DATASET}_java_spark_sample.txt"
  head -20 "$SEQ_OUT" > "$SUMMARY_DIR/samples/${DATASET}_sequential_sample.txt" 2>/dev/null || true

  cat > "$SUMMARY_DIR/README.txt" <<EOF2
Light summary archive for $DATASET.
Includes Hadoop Base, Hadoop InMapper, PySpark Fastest, Java Spark, and Sequential Python.
Distributed jobs read from HDFS.
Sequential Python uses a local temporary copy created from HDFS before timing.
RAM was sampled every 5 seconds from namenode, datanode2, and datanode3.
cluster_memory_summary.csv reports maximum summed RAM used across the three machines.
Each command was protected with a 2-hour timeout, so failed/timeout jobs are logged and the script continues.
EOF2

  tar -czf "$TAR_FILE" -C results/analysis "final_exp_${DATASET}_summary"

  echo "=== SUMMARY CREATED ==="
  ls -lh "$TAR_FILE"
  cat "$SUMMARY_DIR/line_counts.txt"
}

record_csv () {
  dataset="$1"
  method="$2"
  param="$3"
  code="$4"
  sec="$5"
  log="$6"
  mon="$7"
  lines="$8"

  echo "$dataset,$method,$param,$code,$sec,$(parse_wall "$log"),$(parse_rss "$log"),$(cluster_max_gb "$mon"),$lines" >> "$GLOBAL_CSV"
}

run_dataset () {
  DATASET="$1"
  HDFS_INPUT="$2"

  OUT_BASE="/output/final-exp-$DATASET/$DATASET"
  LOG_DIR="results/logs/final_exp_$DATASET"
  ANALYSIS_DIR="results/analysis/final_exp_$DATASET"
  MONITOR_DIR="results/monitor/final_exp_$DATASET"
  SEQ_DIR="/tmp/${DATASET}_seq_local"
  SEQ_OUT="$ANALYSIS_DIR/sequential_outputs/index_${DATASET}.txt"

  echo
  echo "============================================================"
  echo "START DATASET: $DATASET"
  echo "============================================================"

  hdfs dfs -rm -r -f "/output/final-exp-$DATASET"
  rm -rf "$LOG_DIR" "$ANALYSIS_DIR" "$MONITOR_DIR" "results/analysis/final_exp_${DATASET}_summary"
  rm -f "results/analysis/final_exp_${DATASET}_summary_light.tar.gz"

  mkdir -p "$LOG_DIR" "$ANALYSIS_DIR/sequential_outputs" "$MONITOR_DIR"

  {
    echo "Dataset: $DATASET"
    echo "HDFS input: $HDFS_INPUT"
    hdfs dfs -du -s -h "$HDFS_INPUT"
    hdfs dfs -count "$HDFS_INPUT"
  } | tee "$ANALYSIS_DIR/dataset_info.txt"

  echo "=== HADOOP BASE ==="
  for r in $REDUCERS; do
    out="$OUT_BASE/hadoop-base-r$r"
    log="$LOG_DIR/${DATASET}_hadoop-base-r$r.log"
    mon="$MONITOR_DIR/hadoop-base-r$r.csv"

    hdfs dfs -rm -r -f "$out"
    start=$(date +%s)

    run_cmd_with_monitor "$DATASET" "hadoop-base" "r$r" "$log" "$mon" \
      /usr/bin/time -v hadoop jar "$JAR" \
      it.unipi.cloud.InvertedIndex \
      "$HDFS_INPUT" "$out" "$r" "$STOPWORDS_HDFS"

    code=$?
    sec=$(( $(date +%s) - start ))
    lines=$(count_lines "$out")
    echo "lines: $lines"
    record_csv "$DATASET" "hadoop-base" "r$r" "$code" "$sec" "$log" "$mon" "$lines"
  done

  echo "=== HADOOP INMAPPER ==="
  for r in $REDUCERS; do
    out="$OUT_BASE/hadoop-inmapper-r$r"
    log="$LOG_DIR/${DATASET}_hadoop-inmapper-r$r.log"
    mon="$MONITOR_DIR/hadoop-inmapper-r$r.csv"

    hdfs dfs -rm -r -f "$out"
    start=$(date +%s)

    run_cmd_with_monitor "$DATASET" "hadoop-inmapper" "r$r" "$log" "$mon" \
      /usr/bin/time -v hadoop jar "$JAR" \
      it.unipi.cloud.InvertedIndexInMapper \
      "$HDFS_INPUT" "$out" "$r" "$STOPWORDS_HDFS"

    code=$?
    sec=$(( $(date +%s) - start ))
    lines=$(count_lines "$out")
    echo "lines: $lines"
    record_csv "$DATASET" "hadoop-inmapper" "r$r" "$code" "$sec" "$log" "$mon" "$lines"
  done

  echo "=== PYSPARK FASTEST ==="
  for p in $REDUCERS; do
    out="$OUT_BASE/pyspark-fastest-p$p"
    log="$LOG_DIR/${DATASET}_pyspark-fastest-p$p.log"
    mon="$MONITOR_DIR/pyspark-fastest-p$p.csv"

    hdfs dfs -rm -r -f "$out"
    start=$(date +%s)

    run_cmd_with_monitor "$DATASET" "pyspark-fastest" "p$p" "$log" "$mon" \
      /usr/bin/time -v spark-submit \
      --master yarn \
      --deploy-mode client \
      --num-executors 2 \
      --driver-memory 1g \
      --executor-memory 1500m \
      --executor-cores 2 \
      --conf spark.executor.memoryOverhead=384 \
      --conf spark.python.worker.reuse=true \
      "$SPARK_SCRIPT" \
      "$HDFS_INPUT" "hdfs://namenode:9000$out" "$p" "$STOPWORDS_LOCAL"

    code=$?
    sec=$(( $(date +%s) - start ))
    lines=$(count_lines "$out")
    echo "lines: $lines"
    record_csv "$DATASET" "pyspark-fastest" "p$p" "$code" "$sec" "$log" "$mon" "$lines"
  done

  echo "=== JAVA SPARK ==="
  for p in $REDUCERS; do
    out="$OUT_BASE/java-spark-p$p"
    log="$LOG_DIR/${DATASET}_java-spark-p$p.log"
    mon="$MONITOR_DIR/java-spark-p$p.csv"

    hdfs dfs -rm -r -f "$out"
    start=$(date +%s)

    run_cmd_with_monitor "$DATASET" "java-spark" "p$p" "$log" "$mon" \
      /usr/bin/time -v spark-submit \
      --master yarn \
      --deploy-mode client \
      --num-executors 2 \
      --driver-memory 1g \
      --executor-memory 1500m \
      --executor-cores 2 \
      --conf spark.executor.memoryOverhead=384 \
      --conf spark.network.timeout=600s \
      --conf spark.executor.heartbeatInterval=60s \
      --class it.unipi.cloud.JavaSparkInvertedIndex \
      "$JAVA_SPARK_JAR" \
      "$HDFS_INPUT" "hdfs://namenode:9000$out" "$p" "$STOPWORDS_LOCAL"

    code=$?
    sec=$(( $(date +%s) - start ))
    lines=$(count_lines "$out")
    echo "lines: $lines"
    record_csv "$DATASET" "java-spark" "p$p" "$code" "$sec" "$log" "$mon" "$lines"
  done

  echo "=== SEQUENTIAL PYTHON LOCAL BASELINE ==="
  rm -rf "$SEQ_DIR"
  mkdir -p "$SEQ_DIR"
  echo "Copying HDFS input to local temp. This copy is not timed."
  hdfs dfs -get "$HDFS_INPUT"/* "$SEQ_DIR"/

  log="$LOG_DIR/${DATASET}_sequential.log"
  mon="$MONITOR_DIR/sequential-local.csv"

  start=$(date +%s)
  run_cmd_with_monitor "$DATASET" "sequential-python" "local" "$log" "$mon" \
    /usr/bin/time -v python3 "$SEQ_SCRIPT" "$SEQ_DIR" "$SEQ_OUT"

  code=$?
  sec=$(( $(date +%s) - start ))
  lines=$(wc -l < "$SEQ_OUT" 2>/dev/null || echo 0)
  echo "sequential lines: $lines"
  record_csv "$DATASET" "sequential-python" "local" "$code" "$sec" "$log" "$mon" "$lines"

  rm -rf "$SEQ_DIR"

  make_summary "$DATASET"

  echo "DONE DATASET: $DATASET"
}

echo "=== BUILD HADOOP ==="
cd ~/Cloud/hadoop-java && mvn clean package && cd ~/Cloud

echo "=== BUILD JAVA SPARK ==="
cd ~/Cloud/spark-java && mvn clean package && cd ~/Cloud

echo "=== UPLOAD STOPWORDS TO HDFS ==="
hdfs dfs -put -f "$STOPWORDS_LOCAL" "$STOPWORDS_HDFS"


# run_dataset "archive-1gb-20k" "/input/archive-1gb-20k"
run_dataset "gutenberg-large-4472" "/input/gutenberg-large-4472"

echo "============================================================"
echo "ALL DATASETS FINISHED"
echo "============================================================"
cat "$GLOBAL_CSV"
ls -lh results/analysis/final_exp_*_summary_light.tar.gz