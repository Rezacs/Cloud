#!/usr/bin/env bash
set -u

export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export HADOOP_HOME=/opt/hadoop
export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
export YARN_CONF_DIR=/opt/hadoop/etc/hadoop
export SPARK_HOME=/usr/local/spark
export PATH=$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH

cd ~/Cloud

PARTITIONS="1 2 4 8 16 24"

SPARK_SCRIPT="$HOME/Cloud/spark-python/inverted_index_spark.py"
SEQ_SCRIPT="$HOME/Cloud/sequential-python/inverted_index_sequential.py"

STOPWORDS_LOCAL="$HOME/Cloud/hadoop-java/src/main/resources/stopwords.txt"

INPUT_SPARK="hdfs:///input/small/*/*"
LOCAL_INPUT="/var/backups/hadoop/backup_before_reinstall/AllDatasets/Small"

OUT_BASE="/output/final-exp-small"
LOG_DIR="results/logs/final_exp_small"
ANALYSIS_DIR="results/analysis/final_exp_small"
SUMMARY_DIR="results/analysis/final_exp_small_summary"

SEQ_OUT="$ANALYSIS_DIR/sequential_outputs/index_small.txt"
SEQ_LOG="$LOG_DIR/small_sequential.log"

mkdir -p "$LOG_DIR" "$ANALYSIS_DIR" "$(dirname "$SEQ_OUT")"

run_job () {
  label="$1"
  log="$2"
  shift 2

  echo "=== $label ==="
  start=$(date +%s)

  "$@" > "$log" 2>&1
  code=$?

  end=$(date +%s)
  sec=$((end - start))

  echo "Finished $label | exit=$code | seconds=${sec}s"
  echo "Elapsed seconds: $sec" >> "$log"

  if [ "$code" -ne 0 ]; then
    echo "FAILED. Last log lines:"
    tail -n 60 "$log"
    exit "$code"
  fi
}

echo "=== RUN SPARK SMALL ONLY AND REPLACE OLD SPARK OUTPUTS ==="

for p in $PARTITIONS; do
  out="$OUT_BASE/small/spark-optimized-p$p"
  log="$LOG_DIR/small_spark-optimized-p$p.log"

  hdfs dfs -rm -r -f "$out"

  run_job "Spark small p$p" "$log" \
    /usr/bin/time -v spark-submit \
      --master yarn \
      --deploy-mode client \
      --driver-memory 1g \
      --executor-memory 1g \
      --executor-cores 2 \
      "$SPARK_SCRIPT" \
      "$INPUT_SPARK" \
      "hdfs://namenode:9000$out" \
      "$p" \
      "$STOPWORDS_LOCAL"

  echo -n "lines: "
  hdfs dfs -cat "$out/part-*" 2>/dev/null | wc -l
done

echo "=== RUN SEQUENTIAL PYTHON SMALL ==="

run_job "Sequential Python small" "$SEQ_LOG" \
  /usr/bin/time -v python3 "$SEQ_SCRIPT" \
    "$LOCAL_INPUT" \
    "$SEQ_OUT"

echo -n "sequential lines: "
wc -l < "$SEQ_OUT"

echo "=== CREATE LIGHT SUMMARY ARCHIVE FROM EXISTING HADOOP + NEW SPARK + SEQUENTIAL ==="

rm -rf "$SUMMARY_DIR"
mkdir -p "$SUMMARY_DIR/logs" "$SUMMARY_DIR/samples"

cp "$LOG_DIR"/*.log "$SUMMARY_DIR/logs/" 2>/dev/null || true

rm -f "$SUMMARY_DIR/line_counts.txt"

for path in \
  /output/final-exp-small/small/hadoop-base-r1 \
  /output/final-exp-small/small/hadoop-base-r2 \
  /output/final-exp-small/small/hadoop-base-r4 \
  /output/final-exp-small/small/hadoop-base-r8 \
  /output/final-exp-small/small/hadoop-base-r16 \
  /output/final-exp-small/small/hadoop-base-r24 \
  /output/final-exp-small/small/hadoop-inmapper-r1 \
  /output/final-exp-small/small/hadoop-inmapper-r2 \
  /output/final-exp-small/small/hadoop-inmapper-r4 \
  /output/final-exp-small/small/hadoop-inmapper-r8 \
  /output/final-exp-small/small/hadoop-inmapper-r16 \
  /output/final-exp-small/small/hadoop-inmapper-r24 \
  /output/final-exp-small/small/spark-optimized-p1 \
  /output/final-exp-small/small/spark-optimized-p2 \
  /output/final-exp-small/small/spark-optimized-p4 \
  /output/final-exp-small/small/spark-optimized-p8 \
  /output/final-exp-small/small/spark-optimized-p16 \
  /output/final-exp-small/small/spark-optimized-p24
do
  echo -n "$path: " >> "$SUMMARY_DIR/line_counts.txt"
  hdfs dfs -cat "$path/part-*" 2>/dev/null | wc -l >> "$SUMMARY_DIR/line_counts.txt"
done

echo -n "sequential-small: " >> "$SUMMARY_DIR/line_counts.txt"
wc -l < "$SEQ_OUT" >> "$SUMMARY_DIR/line_counts.txt"

hdfs dfs -du -s -h /output/final-exp-small/small/* > "$SUMMARY_DIR/output_sizes.txt" 2>/dev/null
du -h "$SEQ_OUT" >> "$SUMMARY_DIR/output_sizes.txt" 2>/dev/null

cat > "$SUMMARY_DIR/sequential_summary.txt" <<EOF
Sequential Python small dataset
Input: $LOCAL_INPUT
Output: $SEQ_OUT
Lines: $(wc -l < "$SEQ_OUT")
Time:
$(grep -E "Elapsed|Elapsed seconds|User time|System time|Percent of CPU|Maximum resident|Exit status" "$SEQ_LOG")
EOF

{
  for log in "$SUMMARY_DIR/logs"/*.log; do
    [ -f "$log" ] || continue
    echo "--- $(basename "$log") ---"
    grep -E "Elapsed|Elapsed seconds|Maximum resident|User time|System time|Percent of CPU|Launched map tasks|Launched reduce tasks|Map input records|Map output records|Reduce input records|Reduce output records|Job Finished|Job failed|Exception|ERROR|Exit status" "$log"
  done
} > "$SUMMARY_DIR/performance_summary.txt"

hdfs dfs -cat /output/final-exp-small/small/hadoop-inmapper-r1/part-* 2>/dev/null | head -20 > "$SUMMARY_DIR/samples/small_hadoop_inmapper_sample.txt"
hdfs dfs -cat /output/final-exp-small/small/spark-optimized-p1/part-* 2>/dev/null | head -20 > "$SUMMARY_DIR/samples/small_spark_sample.txt"
head -20 "$SEQ_OUT" > "$SUMMARY_DIR/samples/small_sequential_sample.txt"

cat > "$SUMMARY_DIR/README.txt" <<'EOF'
Small dataset light summary archive.
Contains existing Hadoop results, replaced Spark-on-YARN results, sequential Python summary, logs, samples, line counts, and output sizes.
Full sequential index output is excluded to keep archive small.
EOF

tar -czf results/analysis/final_exp_small_summary.tar.gz \
  -C results/analysis final_exp_small_summary

ls -lh results/analysis/final_exp_small_summary.tar.gz

echo "=== DONE ==="