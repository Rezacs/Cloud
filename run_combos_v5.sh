#!/usr/bin/env bash
# ==============================================================================
# run_combos_v5.sh
#
# Runs the SAME 5-method x 6-param experiment matrix as run_all_new_datasets_v5.sh,
# but on 10 *combined* datasets built by passing comma-separated HDFS paths to
# hadoop/spark-submit — no copying or merging of files on HDFS needed.
#
# Uses the same v5 Spark config that worked cleanly on all 13 base datasets:
#   executor-cores=2, executor-memory=3584m+512m overhead,
#   driver-memory=2g, AM=512m, list-status.num-threads=8, ulimit raised.
#
# Output goes to a SEPARATE csv/log so it doesn't clobber your 13-dataset
# results:
#   final_all_experiments_combos_v5.csv
#   anomalies_combos_v5.log
#
# The only structural change vs run_all_new_datasets_v5.sh is in the
# "sequential-python" step: since HDFS_INPUT can now be a comma-separated
# list of paths, each path is `hdfs dfs -get` into its OWN subfolder under
# the local temp dir (src1, src2, ...) to avoid filename collisions between
# datasets, then the sequential script is pointed at the parent dir (it
# already walks recursively — this is exactly what made ds-09 work).
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

VERSION_TAG="combos-v5"
ALL_PARAMS="1 2 4 8 16 24"

JAR="$HOME/Cloud/hadoop-java/target/hadoop-inverted-index-1.0.jar"
SPARK_SCRIPT="$HOME/Cloud/spark-python/spark_inverted_index_fastest.py"
JAVA_SPARK_JAR="$HOME/Cloud/spark-java/target/spark-java-inverted-index-1.0.jar"
SEQ_SCRIPT="$HOME/Cloud/sequential-python/inverted_index_sequential.py"
STOPWORDS_HDFS="/stopwords.txt"
STOPWORDS_LOCAL="$HOME/Cloud/hadoop-java/src/main/resources/stopwords.txt"

SUMMARY_ARCHIVE_DIR="results/analysis/summary_light_archives"
GLOBAL_CSV="$SUMMARY_ARCHIVE_DIR/final_all_experiments_combos_v5.csv"
ANOMALY_LOG="$SUMMARY_ARCHIVE_DIR/anomalies_combos_v5.log"

# ── Spark v5 config (same as run_all_new_datasets_v5.sh, proven good) ──────
SPARK_EXECUTOR_MEMORY="3584m"
SPARK_EXECUTOR_OVERHEAD="512"
SPARK_DRIVER_MEMORY="2g"
SPARK_AM_MEMORY="512m"
SPARK_NUM_EXECUTORS="3"
SPARK_EXECUTOR_CORES="2"
LIST_STATUS_THREADS="8"

# ==============================================================================
# MONITORING (unchanged)
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

# ==============================================================================
# ANOMALY CHECK (unchanged)
# ==============================================================================
check_anomaly () {
  dataset="$1"; method="$2"; param="$3"; code="$4"; lines="$5"; log="$6"; out="$7"

  if [ "$code" != "0" ] || [ "${lines:-0}" = "0" ]; then
    echo "$(date '+%Y-%m-%d %H:%M:%S') | $dataset | $method | $param | exit=$code | lines=${lines:-0}" \
      >> "$ANOMALY_LOG"

    {
      echo
      echo "----- ANOMALY DEBUG ($dataset | $method | $param) -----"
      echo "exit_code=$code lines=${lines:-0}"
      echo "--- hdfs dfs -ls $out ---"
      hdfs dfs -ls "$out" 2>&1 | head -20
      echo "--- hdfs dfs -ls $out (recursive, if dir of dirs) ---"
      hdfs dfs -ls -R "$out" 2>&1 | head -40
      echo "--- first lines of part files (if any) ---"
      hdfs dfs -cat "$out/part-*" 2>&1 | head -5
      echo "--- last 30 lines of job log ---"
      tail -30 "$log"
      echo "----- END ANOMALY DEBUG -----"
    } >> "$log"
  fi
}

record_csv () {
  dataset="$1"; method="$2"; param="$3"; code="$4"; sec="$5"
  log="$6";     mon="$7";    lines="$8"; out="${9:-}"

  yarn_gb=$(yarn_alloc_max_gb "$mon")
  yarn_mb=$(awk "BEGIN {printf \"%.0f\", $yarn_gb * 1024}")

  echo "  >> Peak YARN allocated: ${yarn_gb} GB (${yarn_mb} MB) out of 16200 MB max"

  echo "$dataset,$method,$param,$code,$sec,$(parse_wall "$log"),$(parse_rss "$log"),$yarn_gb,$(system_used_max_gb "$mon"),$lines" \
    >> "$GLOBAL_CSV"

  if [ -n "$out" ]; then
    check_anomaly "$dataset" "$method" "$param" "$code" "$lines" "$log" "$out"
  fi
}

# ==============================================================================
# CLEANUP after each dataset (unchanged)
# ==============================================================================
cleanup_after_dataset () {
  DATASET="$1"
  OUT_BASE="$2"
  LOG_DIR="$3"
  MONITOR_DIR="$4"

  echo "=== CLEANUP after $DATASET ==="

  hdfs dfs -rm -r -f "$OUT_BASE" 2>/dev/null || true

  rm -rf /tmp/hadoop-hadoop/nm-local-dir/*
  ssh hadoop@datanode2 "rm -rf /tmp/hadoop-hadoop/nm-local-dir/*" 2>/dev/null || true
  ssh hadoop@datanode3 "rm -rf /tmp/hadoop-hadoop/nm-local-dir/*" 2>/dev/null || true

  rm -rf "$LOG_DIR" "$MONITOR_DIR"

  echo "  HDFS output deleted"
  echo "  /tmp wiped on all 3 nodes"
  echo "  Namenode disk free: $(df -h / | awk 'NR==2{print $4}')"
  echo "  HDFS remaining: $(hdfs dfsadmin -report 2>/dev/null | grep 'DFS Remaining' | head -1)"
}

# ==============================================================================
# make_summary (unchanged)
# ==============================================================================
make_summary () {
  DATASET="$1"
  LOG_DIR="$2"
  MONITOR_DIR="$3"
  SUMMARY_DIR="results/analysis/final_exp_${DATASET}_${VERSION_TAG}_summary"
  TAR_FILE="$SUMMARY_ARCHIVE_DIR/final_exp_${DATASET}_${VERSION_TAG}_summary_light.tar.gz"

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

  tar -czf "$TAR_FILE" -C results/analysis "final_exp_${DATASET}_${VERSION_TAG}_summary"
  rm -rf "$SUMMARY_DIR"

  echo "  Summary saved: $TAR_FILE ($(du -sh "$TAR_FILE" | cut -f1))"
}

# ==============================================================================
# run_dataset — all 5 methods for one (possibly combined) dataset.
# HDFS_INPUT may be a single path OR a comma-separated list of paths
# (e.g. "/input/ds-02-...,/input/ds-03-..."). Hadoop and Spark both accept
# comma-separated input paths natively, so hadoop-base/inmapper/pyspark/
# java-spark need NO changes. Only the sequential-python step needs to
# fetch each comma-separated path individually.
# ==============================================================================
run_dataset () {
  DATASET="$1"
  HDFS_INPUT="$2"

  OUT_BASE="/output/new-exp-${VERSION_TAG}/$DATASET"
  LOG_DIR="results/logs/${VERSION_TAG}_$DATASET"
  MONITOR_DIR="results/monitor/${VERSION_TAG}_$DATASET"
  SEQ_DIR="/tmp/seq_local_${VERSION_TAG}_$DATASET"
  SEQ_OUT="results/analysis/${VERSION_TAG}_${DATASET}_sequential.txt"

  echo
  echo "============================================================"
  echo "START: $DATASET"
  echo "INPUT: $HDFS_INPUT"
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
    record_csv "$DATASET" "hadoop-base" "r$r" "$code" "$sec" "$log" "$mon" "$lines" "$out"
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
    record_csv "$DATASET" "hadoop-inmapper" "r$r" "$code" "$sec" "$log" "$mon" "$lines" "$out"
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
    record_csv "$DATASET" "pyspark-fastest" "p$p" "$code" "$sec" "$log" "$mon" "$lines" "$out"
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
    record_csv "$DATASET" "java-spark" "p$p" "$code" "$sec" "$log" "$mon" "$lines" "$out"
    hdfs dfs -rm -r -f "$out" 2>/dev/null || true
  done

  # ── Sequential Python ────────────────────────────────────────────────────────
  # HDFS_INPUT may be "/input/a,/input/b,/input/c" — fetch each path into its
  # own subfolder (src1, src2, ...) under SEQ_DIR to avoid filename collisions
  # between datasets, then point the sequential script at SEQ_DIR (it walks
  # recursively, same as it did for ds-09's nested directory).
  echo "=== SEQUENTIAL PYTHON ==="
  rm -rf "$SEQ_DIR" && mkdir -p "$SEQ_DIR" "$(dirname "$SEQ_OUT")"
  echo "  Copying HDFS input(s) to local temp..."
  i=0
  IFS=',' read -ra INPUT_PATHS <<< "$HDFS_INPUT"
  for path in "${INPUT_PATHS[@]}"; do
    i=$((i+1))
    mkdir -p "$SEQ_DIR/src$i"
    hdfs dfs -get "$path"/* "$SEQ_DIR/src$i"/
  done

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

echo "=== CLEAN OLD COMBO OUTPUTS ==="
hdfs dfs -rm -r -f "/output/new-exp-${VERSION_TAG}" 2>/dev/null || true
hdfs dfs -rm -r -f /tmp/*          2>/dev/null || true

echo "=== BUILD HADOOP + SPARK JARS ==="
cd ~/Cloud/hadoop-java && mvn clean package -q && cd ~/Cloud
cd ~/Cloud/spark-java  && mvn clean package -q && cd ~/Cloud

echo "=== UPLOAD STOPWORDS ==="
hdfs dfs -put -f "$STOPWORDS_LOCAL" "$STOPWORDS_HDFS"

mkdir -p "$SUMMARY_ARCHIVE_DIR"
echo "dataset,method,param,exit_status,elapsed_seconds,wall_time,max_process_rss_kb,max_yarn_allocated_gb,system_used_max_gb,lines" \
  > "$GLOBAL_CSV"
: > "$ANOMALY_LOG"

echo
echo "============================================================"
echo "Spark $VERSION_TAG config (same as run_all_new_datasets_v5.sh):"
echo "  executor-memory : $SPARK_EXECUTOR_MEMORY + ${SPARK_EXECUTOR_OVERHEAD}m overhead"
echo "  executor-cores  : $SPARK_EXECUTOR_CORES"
echo "  driver-memory   : $SPARK_DRIVER_MEMORY / AM memory: $SPARK_AM_MEMORY"
echo "  list-status threads: $LIST_STATUS_THREADS"
echo "============================================================"
echo

# ==============================================================================
# 10 COMBINED DATASETS — comma-separated HDFS input paths, no copying needed.
# Ordered roughly smallest -> largest combined size.
# ==============================================================================

# 1. ~559MB / 3224 files — fills the gap between ds-01 (4MB) and ds-04 (339MB)
run_dataset "combo-01-ds02-03-559mb-3224f" \
  "/input/ds-02-261mb-2582files,/input/ds-03-298mb-642files"

# 2. ~848MB / 382 files — same total size as combo-01 but ~10x fewer files
run_dataset "combo-02-ds06-11-848mb-382f" \
  "/input/ds-06-752mb-266files-gutenberg-remaining,/input/ds-11-096mb-116files-archive"

# 3. ~1.9GB / 4988 files — new ~2GB data point, different composition than ds-10
run_dataset "combo-03-ds07-12-1p9gb-4988f" \
  "/input/ds-07-800mb-2493files,/input/ds-12-1p1gb-2495files-gutenberg"

# 4. ~1.15GB / 19680 files — most extreme file-count, modest data volume
run_dataset "combo-04-ds01-09-1p15gb-19680f" \
  "/input/ds-01-004mb-1000files-news-small,/input/ds-09-1p15gb-18680files-copy07-plus-12k-kaggle"

# 5. ~2.7GB / 20597 files — largest combo: max data AND max file count
run_dataset "combo-05-ds09-10-2p7gb-20597f" \
  "/input/ds-09-1p15gb-18680files-copy07-plus-12k-kaggle,/input/ds-10-1p55gb-1917files-combined-04-05-08"

# 6. ~4MB / 1100 files — tiny combo, many small files at small scale
run_dataset "combo-06-ds00-01-004mb-1100f" \
  "/input/ds-00-000mb-100files-tiny-news,/input/ds-01-004mb-1000files-news-small"

# 7. ~1.14GB / 2293 files — 3-way mid-size combo
run_dataset "combo-07-ds03-04-05-1p14gb-2293f" \
  "/input/ds-03-298mb-642files,/input/ds-04-339mb-844files,/input/ds-05-500mb-807files"

# 8. ~1.41GB / 21262 files — medium size + extreme file count together
run_dataset "combo-08-ds02-09-1p41gb-21262f" \
  "/input/ds-02-261mb-2582files,/input/ds-09-1p15gb-18680files-copy07-plus-12k-kaggle"

# 9. ~1.65GB / 2875 files — 3-way combo, gutenberg + archive heavy
run_dataset "combo-09-ds06-07-11-1p65gb-2875f" \
  "/input/ds-06-752mb-266files-gutenberg-remaining,/input/ds-07-800mb-2493files,/input/ds-11-096mb-116files-archive"

# 10. ~3.35GB / 27855 files — biggest stress test: 3 datasets combined
run_dataset "combo-10-ds08-09-12-3p35gb-27855f" \
  "/input/ds-08-1p1gb-6680files,/input/ds-09-1p15gb-18680files-copy07-plus-12k-kaggle,/input/ds-12-1p1gb-2495files-gutenberg"

# ==============================================================================
# FINAL REPORT
# ==============================================================================
echo
echo "============================================================"
echo "ALL COMBO EXPERIMENTS FINISHED"
echo "============================================================"
cat "$GLOBAL_CSV"
echo
echo "Summary archives:"
ls -lh "$SUMMARY_ARCHIVE_DIR"/final_exp_combo-*_${VERSION_TAG}_summary_light.tar.gz 2>/dev/null
echo
if [ -s "$ANOMALY_LOG" ]; then
  echo "============================================================"
  echo "ANOMALIES DETECTED (non-zero exit or 0 output lines):"
  echo "============================================================"
  cat "$ANOMALY_LOG"
  echo
  echo "  -> Check the corresponding *.log files inside the summary tarballs"
  echo "     for the 'ANOMALY DEBUG' section."
else
  echo "No anomalies detected (all jobs exited 0 with non-zero output lines)."
fi
