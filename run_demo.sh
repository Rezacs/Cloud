#!/usr/bin/env bash
# ==============================================================================
# run_demo.sh
#
# DEMO version of run_all_v6.sh — designed for the 15-minute project
# presentation on the cluster.
#
# KEY DIFFERENCES vs run_all_v6.sh:
#
#   1. Only ds-00 and ds-01 run by default (datasets 3-25 are commented out).
#      To run more datasets, uncomment their lines in the "DATASETS" section.
#
#   2. HDFS outputs are KEPT after each job — so you can browse them live
#      on the cluster during the demo (hdfs dfs -ls, hdfs dfs -cat, etc.)
#      Outputs go to /output/demo/ to stay separate from real v6 results.
#
#   3. /tmp is still cleared between datasets (avoids stale shuffle data).
#
#   4. Sequential Python is SKIPPED — it adds no distributed output to show
#      and would consume several minutes of demo time copying files locally.
#
#   5. JAR rebuild (mvn) is SKIPPED — pre-build the JARs the night before.
#      If you need to rebuild, uncomment the mvn lines in the INIT section.
#
#   6. Reduced param sweep for speed:
#        Hadoop: r in {1, 4, 8}   (was 1 2 4 8 16 24)
#        Spark:  p in {4, 16, 32} (was 4 8 16 24 32 40)
#      Enough values to show the trend, finishes in ~5 minutes per dataset.
#
#   7. VERSION_TAG = "demo" so CSV/logs go to separate files and don't
#      overwrite the real v6 results.
# ==============================================================================
set -u

export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export HADOOP_HOME=/opt/hadoop
export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
export YARN_CONF_DIR=/opt/hadoop/etc/hadoop
export SPARK_HOME=/usr/local/spark
export PATH=$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH

ulimit -n 65536 2>/dev/null || true
export HADOOP_CLIENT_OPTS="-Xmx2g ${HADOOP_CLIENT_OPTS:-}"

cd ~/Cloud

VERSION_TAG="demo"

# Reduced sweep for demo speed — uncomment full ranges if you have time
HADOOP_PARAMS="1 4 8"
SPARK_PARAMS="4 16 32"

JAR="$HOME/Cloud/hadoop-java/target/hadoop-inverted-index-1.0.jar"
SPARK_SCRIPT="$HOME/Cloud/spark-python/spark_inverted_index_fastest.py"
JAVA_SPARK_JAR="$HOME/Cloud/spark-java/target/spark-java-inverted-index-1.0.jar"
STOPWORDS_HDFS="/stopwords.txt"
STOPWORDS_LOCAL="$HOME/Cloud/hadoop-java/src/main/resources/stopwords.txt"

SUMMARY_ARCHIVE_DIR="results/analysis/summary_light_archives"
GLOBAL_CSV="$SUMMARY_ARCHIVE_DIR/final_all_experiments_${VERSION_TAG}.csv"
ANOMALY_LOG="$SUMMARY_ARCHIVE_DIR/anomalies_${VERSION_TAG}.log"

# Spark config — same as v6
SPARK_EXECUTOR_MEMORY="3584m"
SPARK_EXECUTOR_OVERHEAD="512"
SPARK_DRIVER_MEMORY="2g"
SPARK_AM_MEMORY="512m"
SPARK_NUM_EXECUTORS="3"
SPARK_EXECUTOR_CORES="2"
LIST_STATUS_THREADS="8"

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
  awk -F, 'NR>1{used[$1]+=$3} END{max=0;for(t in used)if(used[t]>max)max=used[t];printf "%.2f",max/1024}' "$1"
}
system_used_max_gb () {
  awk -F, 'NR>1{used[$1]+=$6} END{max=0;for(t in used)if(used[t]>max)max=used[t];printf "%.2f",max/1024}' "$1"
}
count_lines () { hdfs dfs -cat "$1/part-*" 2>/dev/null | wc -l; }

record_csv () {
  dataset="$1"; method="$2"; param="$3"; code="$4"; sec="$5"
  log="$6"; mon="$7"; lines="$8"

  yarn_gb=$(yarn_alloc_max_gb "$mon")
  yarn_mb=$(awk "BEGIN {printf \"%.0f\", $yarn_gb * 1024}")
  echo "  >> Peak YARN allocated: ${yarn_gb} GB (${yarn_mb} MB)"
  echo "$dataset,$method,$param,$code,$sec,$(parse_wall "$log"),$(parse_rss "$log"),$yarn_gb,$(system_used_max_gb "$mon"),$lines" \
    >> "$GLOBAL_CSV"
}

# ==============================================================================
# DEMO CLEANUP — only wipes /tmp, NEVER deletes HDFS output
# ==============================================================================
cleanup_after_dataset () {
  DATASET="$1"
  LOG_DIR="$2"
  MONITOR_DIR="$3"

  echo "=== CLEANUP /tmp after $DATASET (HDFS output kept for demo) ==="
  rm -rf /tmp/hadoop-hadoop/nm-local-dir/*
  ssh hadoop@datanode2 "rm -rf /tmp/hadoop-hadoop/nm-local-dir/*" 2>/dev/null || true
  ssh hadoop@datanode3 "rm -rf /tmp/hadoop-hadoop/nm-local-dir/*" 2>/dev/null || true
  echo "  /tmp wiped on all 3 nodes"
  echo "  HDFS output KEPT at: /output/${VERSION_TAG}/$DATASET"
  echo "  Browse with: hdfs dfs -ls /output/${VERSION_TAG}/$DATASET"
  echo "  Namenode disk free: $(df -h / | awk 'NR==2{print $4}')"
}

# ==============================================================================
# RUN DATASET — no sequential Python, HDFS output not deleted
# ==============================================================================
run_dataset () {
  DATASET="$1"
  HDFS_INPUT="$2"

  OUT_BASE="/output/${VERSION_TAG}/$DATASET"
  LOG_DIR="results/logs/${VERSION_TAG}_$DATASET"
  MONITOR_DIR="results/monitor/${VERSION_TAG}_$DATASET"

  echo
  echo "============================================================"
  echo "DEMO: $DATASET"
  echo "INPUT: $HDFS_INPUT"
  echo "HADOOP PARAMS : $HADOOP_PARAMS"
  echo "SPARK  PARAMS : $SPARK_PARAMS"
  echo "============================================================"

  # Clear any previous demo run output for this dataset, then recreate
  hdfs dfs -rm -r -f "$OUT_BASE" 2>/dev/null || true
  rm -rf "$LOG_DIR" "$MONITOR_DIR"
  mkdir -p "$LOG_DIR" "$MONITOR_DIR"

  # ── Hadoop Base ─────────────────────────────────────────────────────────────
  echo "=== HADOOP BASE ==="
  for r in $HADOOP_PARAMS; do
    out="$OUT_BASE/hadoop-base-r$r"
    log="$LOG_DIR/${DATASET}_hadoop-base-r$r.log"
    mon="$MONITOR_DIR/hadoop-base-r$r.csv"

    run_cmd_with_yarn_monitor "$DATASET" "hadoop-base" "r$r" "$log" "$mon" \
      /usr/bin/time -v hadoop jar "$JAR" \
        it.unipi.cloud.InvertedIndex \
        "$HDFS_INPUT" "$out" "$r" "$STOPWORDS_HDFS"
    code=$?
    sec=$(grep 'Elapsed seconds' "$log" | awk '{print $3}')
    lines=$(count_lines "$out")
    record_csv "$DATASET" "hadoop-base" "r$r" "$code" "$sec" "$log" "$mon" "$lines"
    echo "  Output kept at: $out ($lines lines)"
  done

  # ── Hadoop InMapper ──────────────────────────────────────────────────────────
  echo "=== HADOOP INMAPPER ==="
  for r in $HADOOP_PARAMS; do
    out="$OUT_BASE/hadoop-inmapper-r$r"
    log="$LOG_DIR/${DATASET}_hadoop-inmapper-r$r.log"
    mon="$MONITOR_DIR/hadoop-inmapper-r$r.csv"

    run_cmd_with_yarn_monitor "$DATASET" "hadoop-inmapper" "r$r" "$log" "$mon" \
      /usr/bin/time -v hadoop jar "$JAR" \
        it.unipi.cloud.InvertedIndexInMapper \
        "$HDFS_INPUT" "$out" "$r" "$STOPWORDS_HDFS"
    code=$?
    sec=$(grep 'Elapsed seconds' "$log" | awk '{print $3}')
    lines=$(count_lines "$out")
    record_csv "$DATASET" "hadoop-inmapper" "r$r" "$code" "$sec" "$log" "$mon" "$lines"
    echo "  Output kept at: $out ($lines lines)"
  done

  # ── PySpark ──────────────────────────────────────────────────────────────────
  echo "=== PYSPARK ==="
  for p in $SPARK_PARAMS; do
    out="$OUT_BASE/pyspark-p$p"
    log="$LOG_DIR/${DATASET}_pyspark-p$p.log"
    mon="$MONITOR_DIR/pyspark-p$p.csv"

    run_cmd_with_yarn_monitor "$DATASET" "pyspark-fastest" "p$p" "$log" "$mon" \
      /usr/bin/time -v spark-submit \
        --master yarn --deploy-mode client \
        --num-executors        "$SPARK_NUM_EXECUTORS" \
        --driver-memory        "$SPARK_DRIVER_MEMORY" \
        --executor-memory      "$SPARK_EXECUTOR_MEMORY" \
        --executor-cores       "$SPARK_EXECUTOR_CORES" \
        --conf "spark.executor.instances=$SPARK_NUM_EXECUTORS" \
        --conf "spark.executor.memoryOverhead=$SPARK_EXECUTOR_OVERHEAD" \
        --conf "spark.yarn.am.memory=$SPARK_AM_MEMORY" \
        --conf spark.dynamicAllocation.enabled=false \
        --conf spark.scheduler.minRegisteredResourcesRatio=1.0 \
        --conf spark.scheduler.maxRegisteredResourcesWaitingTime=60s \
        --conf spark.yarn.am.waitTime=100s \
        --conf spark.python.worker.reuse=true \
        --conf "spark.hadoop.mapreduce.input.fileinputformat.list-status.num-threads=$LIST_STATUS_THREADS" \
        --conf spark.network.timeout=600s \
        --conf spark.executor.heartbeatInterval=60s \
        "$SPARK_SCRIPT" \
        "$HDFS_INPUT" "hdfs://namenode:9000$out" "$p" "$STOPWORDS_LOCAL"
    code=$?
    sec=$(grep 'Elapsed seconds' "$log" | awk '{print $3}')
    lines=$(count_lines "$out")
    record_csv "$DATASET" "pyspark-fastest" "p$p" "$code" "$sec" "$log" "$mon" "$lines"
    echo "  Output kept at: $out ($lines lines)"
  done

  # ── Java Spark ───────────────────────────────────────────────────────────────
  echo "=== JAVA SPARK ==="
  for p in $SPARK_PARAMS; do
    out="$OUT_BASE/java-spark-p$p"
    log="$LOG_DIR/${DATASET}_java-spark-p$p.log"
    mon="$MONITOR_DIR/java-spark-p$p.csv"

    run_cmd_with_yarn_monitor "$DATASET" "java-spark" "p$p" "$log" "$mon" \
      /usr/bin/time -v spark-submit \
        --master yarn --deploy-mode client \
        --num-executors        "$SPARK_NUM_EXECUTORS" \
        --driver-memory        "$SPARK_DRIVER_MEMORY" \
        --executor-memory      "$SPARK_EXECUTOR_MEMORY" \
        --executor-cores       "$SPARK_EXECUTOR_CORES" \
        --conf "spark.executor.instances=$SPARK_NUM_EXECUTORS" \
        --conf "spark.executor.memoryOverhead=$SPARK_EXECUTOR_OVERHEAD" \
        --conf "spark.yarn.am.memory=$SPARK_AM_MEMORY" \
        --conf spark.dynamicAllocation.enabled=false \
        --conf spark.scheduler.minRegisteredResourcesRatio=1.0 \
        --conf spark.scheduler.maxRegisteredResourcesWaitingTime=60s \
        --conf spark.yarn.am.waitTime=100s \
        --conf "spark.hadoop.mapreduce.input.fileinputformat.list-status.num-threads=$LIST_STATUS_THREADS" \
        --conf spark.network.timeout=600s \
        --conf spark.executor.heartbeatInterval=60s \
        --class it.unipi.cloud.JavaSparkInvertedIndex \
        "$JAVA_SPARK_JAR" \
        "$HDFS_INPUT" "hdfs://namenode:9000$out" "$p" "$STOPWORDS_LOCAL"
    code=$?
    sec=$(grep 'Elapsed seconds' "$log" | awk '{print $3}')
    lines=$(count_lines "$out")
    record_csv "$DATASET" "java-spark" "p$p" "$code" "$sec" "$log" "$mon" "$lines"
    echo "  Output kept at: $out ($lines lines)"
  done

  cleanup_after_dataset "$DATASET" "$LOG_DIR" "$MONITOR_DIR"

  echo
  echo "--- DEMO OUTPUTS for $DATASET ---"
  hdfs dfs -ls "/output/${VERSION_TAG}/$DATASET"
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
sleep 3

# JAR REBUILD — commented out for demo (pre-build the night before)
# cd ~/Cloud/hadoop-java && mvn clean package -q && cd ~/Cloud
# cd ~/Cloud/spark-java  && mvn clean package -q && cd ~/Cloud

echo "=== UPLOAD STOPWORDS ==="
hdfs dfs -put -f "$STOPWORDS_LOCAL" "$STOPWORDS_HDFS"

mkdir -p "$SUMMARY_ARCHIVE_DIR"
echo "dataset,method,param,exit_status,elapsed_seconds,wall_time,max_process_rss_kb,max_yarn_allocated_gb,system_used_max_gb,lines" \
  > "$GLOBAL_CSV"
: > "$ANOMALY_LOG"

echo
echo "============================================================"
echo "DEMO config:"
echo "  Hadoop params : $HADOOP_PARAMS"
echo "  Spark  params : $SPARK_PARAMS"
echo "  HDFS outputs  : KEPT at /output/${VERSION_TAG}/"
echo "  Sequential    : SKIPPED"
echo "  JAR rebuild   : SKIPPED (pre-built)"
echo "============================================================"
echo

# ==============================================================================
# DATASETS
# To run more datasets during the demo, uncomment the relevant lines below.
# All 25 datasets are listed in order from fastest to slowest (v6 measured).
# ==============================================================================

# ── ACTIVE (runs by default) ────────────────────────────────────────────────
run_dataset "ds-00-000mb-100files-tiny-news" \
  "/input/ds-00-000mb-100files-tiny-news"

run_dataset "ds-01-004mb-1000files-news-small" \
  "/input/ds-01-004mb-1000files-news-small"

# ── COMMENTED OUT (uncomment to include in demo) ────────────────────────────
# run_dataset "combo-06-ds00-01-004mb-1100f" \
#   "/input/ds-00-000mb-100files-tiny-news,/input/ds-01-004mb-1000files-news-small"

# run_dataset "ds-11-096mb-116files-archive" \
#   "/input/ds-11-096mb-116files-archive"

# run_dataset "ds-04-339mb-844files" \
#   "/input/ds-04-339mb-844files"

# run_dataset "ds-03-298mb-642files" \
#   "/input/ds-03-298mb-642files"

# run_dataset "ds-02-261mb-2582files" \
#   "/input/ds-02-261mb-2582files"

# run_dataset "ds-05-500mb-807files" \
#   "/input/ds-05-500mb-807files"

# run_dataset "ds-06-752mb-266files-gutenberg-remaining" \
#   "/input/ds-06-752mb-266files-gutenberg-remaining"

# run_dataset "combo-02-ds06-11-848mb-382f" \
#   "/input/ds-06-752mb-266files-gutenberg-remaining,/input/ds-11-096mb-116files-archive"

# run_dataset "ds-12-1p1gb-2495files-gutenberg" \
#   "/input/ds-12-1p1gb-2495files-gutenberg"

# run_dataset "ds-07-800mb-2493files" \
#   "/input/ds-07-800mb-2493files"

# run_dataset "ds-08-1p1gb-6680files" \
#   "/input/ds-08-1p1gb-6680files"

# run_dataset "combo-01-ds02-03-559mb-3224f" \
#   "/input/ds-02-261mb-2582files,/input/ds-03-298mb-642files"

# run_dataset "ds-09-1p15gb-18680files-copy07-plus-12k-kaggle" \
#   "/input/ds-09-1p15gb-18680files-copy07-plus-12k-kaggle"

# run_dataset "combo-04-ds01-09-1p15gb-19680f" \
#   "/input/ds-01-004mb-1000files-news-small,/input/ds-09-1p15gb-18680files-copy07-plus-12k-kaggle"

# run_dataset "ds-10-1p55gb-1917files-combined-04-05-08" \
#   "/input/ds-10-1p55gb-1917files-combined-04-05-08"

# run_dataset "combo-09-ds06-07-11-1p65gb-2875f" \
#   "/input/ds-06-752mb-266files-gutenberg-remaining,/input/ds-07-800mb-2493files,/input/ds-11-096mb-116files-archive"

# run_dataset "combo-03-ds07-12-1p9gb-4988f" \
#   "/input/ds-07-800mb-2493files,/input/ds-12-1p1gb-2495files-gutenberg"

# run_dataset "combo-07-ds03-04-05-1p14gb-2293f" \
#   "/input/ds-03-298mb-642files,/input/ds-04-339mb-844files,/input/ds-05-500mb-807files"

# run_dataset "combo-08-ds02-09-1p41gb-21262f" \
#   "/input/ds-02-261mb-2582files,/input/ds-09-1p15gb-18680files-copy07-plus-12k-kaggle"

# run_dataset "combo-11-ds06-07-10-3p1gb-4676f" \
#   "/input/ds-06-752mb-266files-gutenberg-remaining,/input/ds-07-800mb-2493files,/input/ds-10-1p55gb-1917files-combined-04-05-08"

# run_dataset "combo-10-ds08-09-12-3p35gb-27855f" \
#   "/input/ds-08-1p1gb-6680files,/input/ds-09-1p15gb-18680files-copy07-plus-12k-kaggle,/input/ds-12-1p1gb-2495files-gutenberg"

# run_dataset "combo-12-ds08-10-12-3p75gb-11092f" \
#   "/input/ds-08-1p1gb-6680files,/input/ds-10-1p55gb-1917files-combined-04-05-08,/input/ds-12-1p1gb-2495files-gutenberg"

# run_dataset "combo-05-ds09-10-2p7gb-20597f" \
#   "/input/ds-09-1p15gb-18680files-copy07-plus-12k-kaggle,/input/ds-10-1p55gb-1917files-combined-04-05-08"

# ==============================================================================
# FINAL REPORT
# ==============================================================================
echo
echo "============================================================"
echo "DEMO FINISHED"
echo "============================================================"
echo
echo "Results CSV : $GLOBAL_CSV"
cat "$GLOBAL_CSV"
echo
echo "All HDFS outputs:"
hdfs dfs -ls -R /output/${VERSION_TAG}/ | grep "^-" | awk '{print $8}' | head -40
echo
echo "To inspect output for a specific job:"
echo "  hdfs dfs -ls /output/${VERSION_TAG}/ds-00-000mb-100files-tiny-news/"
echo "  hdfs dfs -cat /output/${VERSION_TAG}/ds-00-000mb-100files-tiny-news/hadoop-base-r4/part-r-00000 | head -20"
echo
echo "To clean all demo outputs when done:"
echo "  hdfs dfs -rm -r /output/${VERSION_TAG}/"
