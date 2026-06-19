#!/usr/bin/env bash
# ==============================================================================
# run_all_new_datasets_v5.sh
#
# Runs ALL experiments (hadoop-base, hadoop-inmapper, pyspark-fastest,
# java-spark, sequential-python) on ALL 13 datasets (ds-00 .. ds-12).
#
# CHANGES vs the previous run (v4 -> v5):
#
#   1. SPARK_EXECUTOR_CORES: 1 -> 2
#        Each node has only 2 physical CPU cores, but executors were only
#        ever using 1 of them. Memory request per executor is UNCHANGED,
#        so this is essentially "free" extra parallelism — every executor
#        JVM can now run 2 tasks concurrently and use both cores.
#
#   2. SPARK_EXECUTOR_OVERHEAD: 512 -> 640
#        With 2 cores, PySpark spawns 2 python worker processes per
#        executor instead of 1. Extra off-heap headroom avoids those
#        workers getting squeezed.
#
#   3. SPARK_DRIVER_MEMORY: 1g -> 2g, SPARK_AM_MEMORY: (default 512m) -> 1g
#        In client mode the driver runs on the namenode itself (NOT inside
#        a YARN container), so this costs nothing against the 16.2GB YARN
#        cap and only uses namenode's own free RAM (~4GB free, plenty).
#        This is the main candidate fix for ds-09 (18,680 files): listing
#        and tracking splits for that many files is heavier on driver/AM
#        memory than any other dataset, and is the most likely reason
#        pyspark/java-spark failed (exit 1) after ~30s on every param.
#
#   4. spark.hadoop.mapreduce.input.fileinputformat.list-status.num-threads=8
#        Parallelizes HDFS file listing during input split computation —
#        directly targets datasets with huge file counts (ds-09: 18680
#        files, ds-12: 2495 files, ds-01: 1000 files).
#
#   5. spark.network.timeout / heartbeatInterval added to PySpark too
#        (java-spark already had these) — more headroom before an
#        executor is declared dead while handling many small input files.
#
#   6. ulimit -n raised to 65536 + HADOOP_CLIENT_OPTS bumped
#        Cheap insurance against "too many open files" on the
#        driver/client side for ds-09/ds-12 (large file counts).
#
#   7. New cluster-utilization math:
#        executor container = 3584m heap + 640m overhead = 4224MB
#        3 executors                                     = 12672MB
#        AM container       = 1024m + ~384m overhead     =  1408MB
#        TOTAL                                           = 14080MB
#        out of 16200MB cluster cap (~87%, vs ~82% before).
#        CPU: 3 executors x 2 cores + 1 AM = 7 vcores out of 9 cap
#             (6 of those map 1:1 onto the 6 real physical cores).
#
#   8. ds-11 (96MB/116 files, archive) and ds-12 (1.1GB/2495 files,
#      gutenberg) added to the dataset list.
#
#   9. Anomaly detection: any job that exits non-zero OR produces 0 output
#      lines gets flagged in a separate anomalies_v5.log, with a quick
#      `hdfs dfs -ls`/`-cat | head` dump of the output dir captured into
#      the job's own log for post-mortem — without stopping the run.
#
#   Everything else (6 reducer/partition values: 1 2 4 8 16 24, YARN
#   monitor every 5s, per-dataset cleanup, summary tarballs) is unchanged.
# ==============================================================================
set -u

export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export HADOOP_HOME=/opt/hadoop
export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
export YARN_CONF_DIR=/opt/hadoop/etc/hadoop
export SPARK_HOME=/usr/local/spark
export PATH=$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH

# Cheap insurance for the many-small-files datasets (ds-01, ds-09, ds-12):
# avoid EMFILE on the driver/client side and give the HDFS client more heap
# when listing/getting thousands of files.
ulimit -n 65536 2>/dev/null || true
export HADOOP_CLIENT_OPTS="-Xmx2g ${HADOOP_CLIENT_OPTS:-}"

cd ~/Cloud

VERSION_TAG="v5"
ALL_PARAMS="1 2 4 8 16 24"

JAR="$HOME/Cloud/hadoop-java/target/hadoop-inverted-index-1.0.jar"
SPARK_SCRIPT="$HOME/Cloud/spark-python/spark_inverted_index_fastest.py"
JAVA_SPARK_JAR="$HOME/Cloud/spark-java/target/spark-java-inverted-index-1.0.jar"
SEQ_SCRIPT="$HOME/Cloud/sequential-python/inverted_index_sequential.py"
STOPWORDS_HDFS="/stopwords.txt"
STOPWORDS_LOCAL="$HOME/Cloud/hadoop-java/src/main/resources/stopwords.txt"

SUMMARY_ARCHIVE_DIR="results/analysis/summary_light_archives"
GLOBAL_CSV="$SUMMARY_ARCHIVE_DIR/final_all_experiments_${VERSION_TAG}.csv"
ANOMALY_LOG="$SUMMARY_ARCHIVE_DIR/anomalies_${VERSION_TAG}.log"

# ── Spark v5 config ────────────────────────────────────────────────────────
SPARK_EXECUTOR_MEMORY="3584m"
SPARK_EXECUTOR_OVERHEAD="512"     # was 640 -> revert
SPARK_DRIVER_MEMORY="2g"          # keep, runs outside YARN, harmless
SPARK_AM_MEMORY="512m"            # was 1g -> revert to default-ish
SPARK_NUM_EXECUTORS="3"
SPARK_EXECUTOR_CORES="2"          # keep
LIST_STATUS_THREADS="8"           # keep

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

# ==============================================================================
# ANOMALY CHECK — flags failures or empty output without stopping the run.
# When something looks wrong, append a note to ANOMALY_LOG and dump a quick
# `hdfs dfs -ls` / head of the output into the job's own log for inspection.
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
# run_dataset — all 5 methods for one dataset
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
echo "Spark $VERSION_TAG config:"
echo "  executor-memory : $SPARK_EXECUTOR_MEMORY + ${SPARK_EXECUTOR_OVERHEAD}m overhead = $((${SPARK_EXECUTOR_MEMORY%m}+SPARK_EXECUTOR_OVERHEAD))MB/executor"
echo "  executor-cores  : $SPARK_EXECUTOR_CORES (was 1 — now uses both physical cores/node)"
echo "  driver-memory   : $SPARK_DRIVER_MEMORY (runs on namenode, outside YARN cap)"
echo "  AM memory       : $SPARK_AM_MEMORY (+ ~384m overhead)"
echo "  3 executors + AM: $((3*(${SPARK_EXECUTOR_MEMORY%m}+SPARK_EXECUTOR_OVERHEAD) + (${SPARK_AM_MEMORY%g}*1024+384) ))MB / 16200MB cluster"
echo "  dynamic alloc   : DISABLED — all 3 executors forced from start"
echo "  list-status threads: $LIST_STATUS_THREADS (helps ds-01/ds-09/ds-12 with many input files)"
echo "============================================================"
echo

# ==============================================================================
# ALL DATASETS — in order from smallest to largest
# ==============================================================================
# run_dataset "ds-00-000mb-100files-tiny-news"       "/input/ds-00-000mb-100files-tiny-news"
# run_dataset "ds-01-004mb-1000files-news-small"     "/input/ds-01-004mb-1000files-news-small"
# run_dataset "ds-02-261mb-2582files"                "/input/ds-02-261mb-2582files"
# run_dataset "ds-03-298mb-642files"                 "/input/ds-03-298mb-642files"
# run_dataset "ds-04-339mb-844files"                 "/input/ds-04-339mb-844files"
# run_dataset "ds-05-500mb-807files"                 "/input/ds-05-500mb-807files"
# run_dataset "ds-06-752mb-266files-gutenberg-remaining" "/input/ds-06-752mb-266files-gutenberg-remaining"
# run_dataset "ds-07-800mb-2493files"                "/input/ds-07-800mb-2493files"
# run_dataset "ds-08-1p1gb-6680files"                "/input/ds-08-1p1gb-6680files"
run_dataset "ds-09-1p15gb-18680files-copy07-plus-12k-kaggle" "/input/ds-09-1p15gb-18680files-copy07-plus-12k-kaggle"
# run_dataset "ds-10-1p55gb-1917files-combined-04-05-08" "/input/ds-10-1p55gb-1917files-combined-04-05-08"
# run_dataset "ds-11-096mb-116files-archive"         "/input/ds-11-096mb-116files-archive"
# run_dataset "ds-12-1p1gb-2495files-gutenberg"      "/input/ds-12-1p1gb-2495files-gutenberg"

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
ls -lh "$SUMMARY_ARCHIVE_DIR"/final_exp_*_${VERSION_TAG}_summary_light.tar.gz 2>/dev/null
echo
if [ -s "$ANOMALY_LOG" ]; then
  echo "============================================================"
  echo "ANOMALIES DETECTED (non-zero exit or 0 output lines):"
  echo "============================================================"
  cat "$ANOMALY_LOG"
  echo
  echo "  -> Check the corresponding *.log files inside the summary tarballs"
  echo "     for the 'ANOMALY DEBUG' section with hdfs ls / tail of the job log."
else
  echo "No anomalies detected (all jobs exited 0 with non-zero output lines)."
fi
