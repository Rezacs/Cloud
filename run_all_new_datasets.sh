#!/usr/bin/env bash
# ==============================================================================
# run_all_new_datasets.sh
#
# Runs ALL experiments (hadoop-base, hadoop-inmapper, pyspark-fastest,
# java-spark, sequential-python) on all 11 new datasets.
#
# Key settings:
#   - 6 reducer/partition values: 1 2 4 8 16 24
#   - Spark v4 config: dynamic allocation OFF, 3 executors forced,
#     3584m heap + 512m overhead = 4096MB container, 82% cluster usage
#   - YARN monitor prints peak GB out of 16200MB after every job
#   - After each dataset: HDFS output deleted, /tmp wiped on all 3 nodes,
#     local logs/monitor deleted → servers stay clean throughout the night
#   - Summary .tar.gz kept in results/analysis/summary_light_archives/
# ==============================================================================
set -u

export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export HADOOP_HOME=/opt/hadoop
export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
export YARN_CONF_DIR=/opt/hadoop/etc/hadoop
export SPARK_HOME=/usr/local/spark
export PATH=$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH

cd ~/Cloud

ALL_PARAMS="1 2 4 8 16 24"

JAR="$HOME/Cloud/hadoop-java/target/hadoop-inverted-index-1.0.jar"
SPARK_SCRIPT="$HOME/Cloud/spark-python/spark_inverted_index_fastest.py"
JAVA_SPARK_JAR="$HOME/Cloud/spark-java/target/spark-java-inverted-index-1.0.jar"
SEQ_SCRIPT="$HOME/Cloud/sequential-python/inverted_index_sequential.py"
STOPWORDS_HDFS="/stopwords.txt"
STOPWORDS_LOCAL="$HOME/Cloud/hadoop-java/src/main/resources/stopwords.txt"

SUMMARY_ARCHIVE_DIR="results/analysis/summary_light_archives"
GLOBAL_CSV="$SUMMARY_ARCHIVE_DIR/final_all_experiments_new_datasets.csv"

# ── Spark v4 config ───────────────────────────────────────────────────────────
SPARK_EXECUTOR_MEMORY="3584m"
SPARK_EXECUTOR_OVERHEAD="512"
SPARK_DRIVER_MEMORY="1g"
SPARK_NUM_EXECUTORS="3"
SPARK_EXECUTOR_CORES="1"

# ==============================================================================
# MONITORING
# ==============================================================================
run_cmd_with_yarn_monitor () {
  dataset="$1"; method="$2"; param="$3"; log="$4"; mon="$5"
  shift 5

  echo "=== $dataset | $method | $param ==="
  echo "timestamp,node_id,used_yarn_mb,configured_yarn_mb,running_containers,system_used_mb,system_available_mb" \
    > "$mon"

  (
    while true; do
      ts=$(date "+%Y-%m-%d %H:%M:%S")
      yarn node -list -all 2>/dev/null | awk '$2=="RUNNING" {print $1}' | while read nodeid; do
        host=$(echo "$nodeid" | cut -d: -f1)
        status=$(yarn node -status "$nodeid" 2>/dev/null)
        used_yarn=$(echo "$status"       | awk -F':|MB' '/Memory-Used/     {gsub(/ /,"",$2); print $2}' | tail -1)
        configured_yarn=$(echo "$status" | awk -F':|MB' '/Memory-Capacity/ {gsub(/ /,"",$2); print $2}' | tail -1)
        containers=$(echo "$status"      | awk -F':'    '/Containers/       {gsub(/ /,"",$2); print $2}' | tail -1)
        sysmem=$(ssh hadoop@"$host" "free -m | awk '/^Mem:/ {print \$3\",\"\$7}'" 2>/dev/null)
        system_used=$(echo "$sysmem"      | cut -d, -f1)
        system_available=$(echo "$sysmem" | cut -d, -f2)
        echo "$ts,$nodeid,${used_yarn:-0},${configured_yarn:-0},${containers:-0},${system_used:-0},${system_available:-0}"
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

parse_wall () { grep -E "Elapsed \(wall clock\)" "$1" | tail -1 | awk -F': ' '{print $2}'; }
parse_rss  () { grep -E "Maximum resident"        "$1" | tail -1 | awk '{print $6}'; }

yarn_alloc_max_gb () {
  awk -F, '
    NR > 1 { used[$1] += $3 }
    END { max=0; for(t in used) if(used[t]>max) max=used[t]; printf "%.2f", max/1024 }
  ' "$1"
}

system_used_max_gb () {
  awk -F, '
    NR > 1 { used[$1] += $6 }
    END { max=0; for(t in used) if(used[t]>max) max=used[t]; printf "%.2f", max/1024 }
  ' "$1"
}

count_lines () { hdfs dfs -cat "$1/part-*" 2>/dev/null | wc -l; }

record_csv () {
  dataset="$1"; method="$2"; param="$3"; code="$4"; sec="$5"
  log="$6";     mon="$7";    lines="$8"

  yarn_gb=$(yarn_alloc_max_gb "$mon")
  yarn_mb=$(awk "BEGIN {printf \"%.0f\", $yarn_gb * 1024}")

  echo "  >> Peak YARN allocated: ${yarn_gb} GB (${yarn_mb} MB) out of 16200 MB max"

  echo "$dataset,$method,$param,$code,$sec,$(parse_wall "$log"),$(parse_rss "$log"),$yarn_gb,$(system_used_max_gb "$mon"),$lines" \
    >> "$GLOBAL_CSV"
}

# ==============================================================================
# CLEANUP after each dataset
# ==============================================================================
cleanup_after_dataset () {
  DATASET="$1"
  OUT_BASE="$2"
  LOG_DIR="$3"
  MONITOR_DIR="$4"

  echo "=== CLEANUP after $DATASET ==="

  # Delete HDFS outputs
  hdfs dfs -rm -r -f "$OUT_BASE" 2>/dev/null || true

  # Wipe /tmp nm-local-dir on all 3 nodes (Spark shuffle + YARN container cache)
  rm -rf /tmp/hadoop-hadoop/nm-local-dir/*
  ssh hadoop@datanode2 "rm -rf /tmp/hadoop-hadoop/nm-local-dir/*" 2>/dev/null || true
  ssh hadoop@datanode3 "rm -rf /tmp/hadoop-hadoop/nm-local-dir/*" 2>/dev/null || true

  # Delete local logs and monitor
  rm -rf "$LOG_DIR" "$MONITOR_DIR"

  echo "  HDFS output deleted"
  echo "  /tmp wiped on all 3 nodes"
  echo "  Namenode disk free: $(df -h / | awk 'NR==2{print $4}')"
  echo "  HDFS remaining: $(hdfs dfsadmin -report 2>/dev/null | grep 'DFS Remaining' | head -1)"
}

# ==============================================================================
# make_summary — archive key results before cleanup
# ==============================================================================
make_summary () {
  DATASET="$1"
  LOG_DIR="$2"
  MONITOR_DIR="$3"
  SUMMARY_DIR="results/analysis/final_exp_${DATASET}_summary"
  TAR_FILE="$SUMMARY_ARCHIVE_DIR/final_exp_${DATASET}_summary_light.tar.gz"

  rm -rf "$SUMMARY_DIR"
  mkdir -p "$SUMMARY_DIR/logs" "$SUMMARY_DIR/yarn_monitor"

  cp "$LOG_DIR"/*.log     "$SUMMARY_DIR/logs/"       2>/dev/null || true
  cp "$MONITOR_DIR"/*.csv "$SUMMARY_DIR/yarn_monitor/" 2>/dev/null || true

  {
    echo "job,max_yarn_allocated_gb,system_used_max_gb"
    for csv in "$SUMMARY_DIR"/yarn_monitor/*.csv; do
      [ -f "$csv" ] || continue
      job=$(basename "$csv" .csv)
      echo "$job,$(yarn_alloc_max_gb "$csv"),$(system_used_max_gb "$csv")"
    done
  } > "$SUMMARY_DIR/yarn_summary.csv"

  {
    for log in "$SUMMARY_DIR/logs"/*.log; do
      [ -f "$log" ] || continue
      echo "--- $(basename "$log") ---"
      grep -E "Elapsed|elapsed|Maximum resident|Exit status|ERROR|Exception|failed" "$log" 2>/dev/null
    done
  } > "$SUMMARY_DIR/performance_summary.txt"

  tar -czf "$TAR_FILE" -C results/analysis "final_exp_${DATASET}_summary"
  rm -rf "$SUMMARY_DIR"

  echo "  Summary saved: $TAR_FILE ($(du -sh "$TAR_FILE" | cut -f1))"
}

# ==============================================================================
# run_dataset — all 5 methods for one dataset
# ==============================================================================
run_dataset () {
  DATASET="$1"
  HDFS_INPUT="$2"

  OUT_BASE="/output/new-exp/$DATASET"
  LOG_DIR="results/logs/new_exp_$DATASET"
  MONITOR_DIR="results/monitor/new_exp_$DATASET"
  SEQ_DIR="/tmp/seq_local_$DATASET"
  SEQ_OUT="results/analysis/new_exp_${DATASET}_sequential.txt"

  echo
  echo "============================================================"
  echo "START: $DATASET"
  echo "PARAMS: $ALL_PARAMS"
  echo "============================================================"

  hdfs dfs -rm -r -f "$OUT_BASE" 2>/dev/null || true
  rm -rf "$LOG_DIR" "$MONITOR_DIR"
  mkdir -p "$LOG_DIR" "$MONITOR_DIR" "results/analysis"

  # ── Hadoop Base ──────────────────────────────────────────────────────────────
  echo "=== HADOOP BASE ==="
  for r in $ALL_PARAMS; do
    out="$OUT_BASE/hadoop-base-r$r"
    log="$LOG_DIR/${DATASET}_hadoop-base-r$r.log"
    mon="$MONITOR_DIR/hadoop-base-r$r.csv"
    hdfs dfs -rm -r -f "$out"

    run_cmd_with_yarn_monitor "$DATASET" "hadoop-base" "r$r" "$log" "$mon" \
      /usr/bin/time -v hadoop jar "$JAR" \
        it.unipi.cloud.InvertedIndex \
        "$HDFS_INPUT" "$out" "$r" "$STOPWORDS_HDFS"
    code=$?
    sec=$(grep 'Elapsed seconds' "$log" | awk '{print $3}')
    lines=$(count_lines "$out")
    record_csv "$DATASET" "hadoop-base" "r$r" "$code" "$sec" "$log" "$mon" "$lines"
    hdfs dfs -rm -r -f "$out" 2>/dev/null || true
  done

  # ── Hadoop InMapper ──────────────────────────────────────────────────────────
  echo "=== HADOOP INMAPPER ==="
  for r in $ALL_PARAMS; do
    out="$OUT_BASE/hadoop-inmapper-r$r"
    log="$LOG_DIR/${DATASET}_hadoop-inmapper-r$r.log"
    mon="$MONITOR_DIR/hadoop-inmapper-r$r.csv"
    hdfs dfs -rm -r -f "$out"

    run_cmd_with_yarn_monitor "$DATASET" "hadoop-inmapper" "r$r" "$log" "$mon" \
      /usr/bin/time -v hadoop jar "$JAR" \
        it.unipi.cloud.InvertedIndexInMapper \
        "$HDFS_INPUT" "$out" "$r" "$STOPWORDS_HDFS"
    code=$?
    sec=$(grep 'Elapsed seconds' "$log" | awk '{print $3}')
    lines=$(count_lines "$out")
    record_csv "$DATASET" "hadoop-inmapper" "r$r" "$code" "$sec" "$log" "$mon" "$lines"
    hdfs dfs -rm -r -f "$out" 2>/dev/null || true
  done

  # ── PySpark ──────────────────────────────────────────────────────────────────
  echo "=== PYSPARK FASTEST ==="
  for p in $ALL_PARAMS; do
    out="$OUT_BASE/pyspark-p$p"
    log="$LOG_DIR/${DATASET}_pyspark-p$p.log"
    mon="$MONITOR_DIR/pyspark-p$p.csv"
    hdfs dfs -rm -r -f "$out"

    run_cmd_with_yarn_monitor "$DATASET" "pyspark-fastest" "p$p" "$log" "$mon" \
      /usr/bin/time -v spark-submit \
        --master yarn \
        --deploy-mode client \
        --num-executors        "$SPARK_NUM_EXECUTORS" \
        --driver-memory        "$SPARK_DRIVER_MEMORY" \
        --executor-memory      "$SPARK_EXECUTOR_MEMORY" \
        --executor-cores       "$SPARK_EXECUTOR_CORES" \
        --conf "spark.executor.instances=$SPARK_NUM_EXECUTORS" \
        --conf "spark.executor.memoryOverhead=$SPARK_EXECUTOR_OVERHEAD" \
        --conf spark.dynamicAllocation.enabled=false \
        --conf spark.scheduler.minRegisteredResourcesRatio=1.0 \
        --conf spark.scheduler.maxRegisteredResourcesWaitingTime=60s \
        --conf spark.yarn.am.waitTime=100s \
        --conf spark.python.worker.reuse=true \
        "$SPARK_SCRIPT" \
        "$HDFS_INPUT" "hdfs://namenode:9000$out" "$p" "$STOPWORDS_LOCAL"
    code=$?
    sec=$(grep 'Elapsed seconds' "$log" | awk '{print $3}')
    lines=$(count_lines "$out")
    record_csv "$DATASET" "pyspark-fastest" "p$p" "$code" "$sec" "$log" "$mon" "$lines"
    hdfs dfs -rm -r -f "$out" 2>/dev/null || true
  done

  # ── Java Spark ───────────────────────────────────────────────────────────────
  echo "=== JAVA SPARK ==="
  for p in $ALL_PARAMS; do
    out="$OUT_BASE/java-spark-p$p"
    log="$LOG_DIR/${DATASET}_java-spark-p$p.log"
    mon="$MONITOR_DIR/java-spark-p$p.csv"
    hdfs dfs -rm -r -f "$out"

    run_cmd_with_yarn_monitor "$DATASET" "java-spark" "p$p" "$log" "$mon" \
      /usr/bin/time -v spark-submit \
        --master yarn \
        --deploy-mode client \
        --num-executors        "$SPARK_NUM_EXECUTORS" \
        --driver-memory        "$SPARK_DRIVER_MEMORY" \
        --executor-memory      "$SPARK_EXECUTOR_MEMORY" \
        --executor-cores       "$SPARK_EXECUTOR_CORES" \
        --conf "spark.executor.instances=$SPARK_NUM_EXECUTORS" \
        --conf "spark.executor.memoryOverhead=$SPARK_EXECUTOR_OVERHEAD" \
        --conf spark.dynamicAllocation.enabled=false \
        --conf spark.scheduler.minRegisteredResourcesRatio=1.0 \
        --conf spark.scheduler.maxRegisteredResourcesWaitingTime=60s \
        --conf spark.yarn.am.waitTime=100s \
        --conf spark.network.timeout=600s \
        --conf spark.executor.heartbeatInterval=60s \
        --class it.unipi.cloud.JavaSparkInvertedIndex \
        "$JAVA_SPARK_JAR" \
        "$HDFS_INPUT" "hdfs://namenode:9000$out" "$p" "$STOPWORDS_LOCAL"
    code=$?
    sec=$(grep 'Elapsed seconds' "$log" | awk '{print $3}')
    lines=$(count_lines "$out")
    record_csv "$DATASET" "java-spark" "p$p" "$code" "$sec" "$log" "$mon" "$lines"
    hdfs dfs -rm -r -f "$out" 2>/dev/null || true
  done

  # ── Sequential Python ────────────────────────────────────────────────────────
  echo "=== SEQUENTIAL PYTHON ==="
  rm -rf "$SEQ_DIR" && mkdir -p "$SEQ_DIR" "$(dirname "$SEQ_OUT")"
  echo "  Copying HDFS input to local temp..."
  hdfs dfs -get "$HDFS_INPUT"/* "$SEQ_DIR"/

  log="$LOG_DIR/${DATASET}_sequential.log"
  mon="$MONITOR_DIR/sequential.csv"

  run_cmd_with_yarn_monitor "$DATASET" "sequential-python" "local" "$log" "$mon" \
    /usr/bin/time -v python3 "$SEQ_SCRIPT" "$SEQ_DIR" "$SEQ_OUT"
  code=$?
  sec=$(grep 'Elapsed seconds' "$log" | awk '{print $3}')
  lines=$(wc -l < "$SEQ_OUT" 2>/dev/null || echo 0)
  record_csv "$DATASET" "sequential-python" "local" "$code" "$sec" "$log" "$mon" "$lines"
  rm -rf "$SEQ_DIR" "$SEQ_OUT"

  # ── Summary + Cleanup ────────────────────────────────────────────────────────
  make_summary   "$DATASET" "$LOG_DIR" "$MONITOR_DIR"
  cleanup_after_dataset "$DATASET" "$OUT_BASE" "$LOG_DIR" "$MONITOR_DIR"

  echo "DONE: $DATASET"
  echo "============================================================"
}

# ==============================================================================
# INIT
# ==============================================================================
echo "=== KILL ANY RUNNING JOBS ==="
yarn application -list 2>/dev/null | awk '/application_/ {print $1}' | xargs -r yarn application -kill
pkill -f spark-submit 2>/dev/null || true
pkill -f SparkSubmit  2>/dev/null || true
pkill -f RunJar       2>/dev/null || true
sleep 5

echo "=== CLEAN OLD OUTPUTS ==="
hdfs dfs -rm -r -f /output/new-exp 2>/dev/null || true
hdfs dfs -rm -r -f /tmp/*          2>/dev/null || true

echo "=== BUILD HADOOP + SPARK JARS ==="
cd ~/Cloud/hadoop-java && mvn clean package -q && cd ~/Cloud
cd ~/Cloud/spark-java  && mvn clean package -q && cd ~/Cloud

echo "=== UPLOAD STOPWORDS ==="
hdfs dfs -put -f "$STOPWORDS_LOCAL" "$STOPWORDS_HDFS"

mkdir -p "$SUMMARY_ARCHIVE_DIR"
echo "dataset,method,param,exit_status,elapsed_seconds,wall_time,max_process_rss_kb,max_yarn_allocated_gb,system_used_max_gb,lines" \
  > "$GLOBAL_CSV"

echo
echo "============================================================"
echo "Spark v4 config:"
echo "  executor-memory : $SPARK_EXECUTOR_MEMORY + ${SPARK_EXECUTOR_OVERHEAD}m overhead = $((${SPARK_EXECUTOR_MEMORY%m}+SPARK_EXECUTOR_OVERHEAD))MB/executor"
echo "  3 executors + AM: $((3*(${SPARK_EXECUTOR_MEMORY%m}+SPARK_EXECUTOR_OVERHEAD)+1024))MB / 16200MB cluster (82%)"
echo "  dynamic alloc   : DISABLED — all 3 executors forced from start"
echo "  YARN max shown  : ~16.2 GB across 3 nodes"
echo "============================================================"
echo

# ==============================================================================
# ALL DATASETS — in order from smallest to largest
# ==============================================================================
run_dataset "ds-00-000mb-100files-tiny-news"       "/input/ds-00-000mb-100files-tiny-news"
run_dataset "ds-01-004mb-1000files-news-small"     "/input/ds-01-004mb-1000files-news-small"
run_dataset "ds-02-261mb-2582files"                "/input/ds-02-261mb-2582files"
run_dataset "ds-03-298mb-642files"                 "/input/ds-03-298mb-642files"
run_dataset "ds-04-339mb-844files"                 "/input/ds-04-339mb-844files"
run_dataset "ds-05-500mb-807files"                 "/input/ds-05-500mb-807files"
run_dataset "ds-06-752mb-266files-gutenberg-remaining" "/input/ds-06-752mb-266files-gutenberg-remaining"
run_dataset "ds-07-800mb-2493files"                "/input/ds-07-800mb-2493files"
run_dataset "ds-08-1p1gb-6680files"                "/input/ds-08-1p1gb-6680files"
run_dataset "ds-09-1p15gb-18680files-copy07-plus-12k-kaggle" "/input/ds-09-1p15gb-18680files-copy07-plus-12k-kaggle"
run_dataset "ds-10-1p55gb-1917files-combined-04-05-08" "/input/ds-10-1p55gb-1917files-combined-04-05-08"

# ==============================================================================
# FINAL REPORT
# ==============================================================================
echo
echo "============================================================"
echo "ALL EXPERIMENTS FINISHED"
echo "============================================================"
cat "$GLOBAL_CSV"
echo
echo "Summary archives:"
ls -lh "$SUMMARY_ARCHIVE_DIR"/final_exp_*_summary_light.tar.gz 2>/dev/null
