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

JAR="$HOME/Cloud/hadoop-java/target/hadoop-inverted-index-1.0.jar"
SPARK_SCRIPT="$HOME/Cloud/spark-python/inverted_index_spark.py"
SEQ_SCRIPT="$HOME/Cloud/sequential-python/inverted_index_sequential.py"

STOPWORDS_HDFS="/stopwords.txt"
STOPWORDS_LOCAL="$HOME/Cloud/hadoop-java/src/main/resources/stopwords.txt"

INPUT_HADOOP="/input/gutenberg-medium"
INPUT_SPARK="hdfs:///input/gutenberg-medium"
LOCAL_INPUT="/var/backups/hadoop/backup_before_reinstall/gutenberg/medium"

OUT_BASE="/output/final-exp-medium"
LOG_DIR="results/logs/final_exp_medium"
ANALYSIS_DIR="results/analysis/final_exp_medium"
SUMMARY_DIR="results/analysis/final_exp_medium_summary"
SEQ_OUT="results/analysis/final_exp_medium/sequential_outputs/index_medium.txt"

mkdir -p "$LOG_DIR" "$ANALYSIS_DIR" "$(dirname "$SEQ_OUT")"

echo "=== CLEAN ==="
hdfs dfs -rm -r -f "$OUT_BASE"
rm -rf "$SUMMARY_DIR"
rm -f "$ANALYSIS_DIR/line_counts.txt" "$ANALYSIS_DIR/output_sizes.txt" "$ANALYSIS_DIR/performance_summary.txt"

echo "=== BUILD ==="
cd ~/Cloud/hadoop-java && mvn clean package && cd ~/Cloud

echo "=== UPLOAD STOPWORDS ==="
hdfs dfs -put -f "$STOPWORDS_LOCAL" "$STOPWORDS_HDFS"

echo "=== DATASET CHECK ===" | tee "$ANALYSIS_DIR/dataset_info.txt"
hdfs dfs -count /input/gutenberg-medium | tee -a "$ANALYSIS_DIR/dataset_info.txt"
hdfs dfs -du -s -h /input/gutenberg-medium | tee -a "$ANALYSIS_DIR/dataset_info.txt"
echo "Local input: $LOCAL_INPUT" | tee -a "$ANALYSIS_DIR/dataset_info.txt"
find "$LOCAL_INPUT" -type f -name "*.txt" | wc -l | tee -a "$ANALYSIS_DIR/dataset_info.txt"
du -sh "$LOCAL_INPUT" | tee -a "$ANALYSIS_DIR/dataset_info.txt"

echo "=== CLUSTER CHECK ===" | tee "$ANALYSIS_DIR/cluster_info.txt"
yarn node -list -all | tee -a "$ANALYSIS_DIR/cluster_info.txt"
hdfs dfsadmin -report | grep -E "Live datanodes|Hostname|DFS Used|DFS Remaining" | tee -a "$ANALYSIS_DIR/cluster_info.txt"

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

echo "=== START HADOOP BASE EXPERIMENTS ==="

for r in $REDUCERS; do
  out="$OUT_BASE/medium/hadoop-base-r$r"
  log="$LOG_DIR/medium_hadoop-base-r$r.log"
  hdfs dfs -rm -r -f "$out"

  run_job "Hadoop BASE medium r$r" "$log" \
    /usr/bin/time -v hadoop jar "$JAR" \
    it.unipi.cloud.InvertedIndex \
    "$INPUT_HADOOP" "$out" "$r" "$STOPWORDS_HDFS"

  lines=$(hdfs dfs -cat "$out/part-*" 2>/dev/null | wc -l)
  echo "$out: $lines" | tee -a "$ANALYSIS_DIR/line_counts.txt"
done

echo "=== START HADOOP INMAPPER EXPERIMENTS ==="

for r in $REDUCERS; do
  out="$OUT_BASE/medium/hadoop-inmapper-r$r"
  log="$LOG_DIR/medium_hadoop-inmapper-r$r.log"
  hdfs dfs -rm -r -f "$out"

  run_job "Hadoop INMAPPER medium r$r" "$log" \
    /usr/bin/time -v hadoop jar "$JAR" \
    it.unipi.cloud.InvertedIndexInMapper \
    "$INPUT_HADOOP" "$out" "$r" "$STOPWORDS_HDFS"

  lines=$(hdfs dfs -cat "$out/part-*" 2>/dev/null | wc -l)
  echo "$out: $lines" | tee -a "$ANALYSIS_DIR/line_counts.txt"
done

echo "=== START SPARK EXPERIMENTS ==="

for p in $REDUCERS; do
  out="$OUT_BASE/medium/spark-optimized-p$p"
  log="$LOG_DIR/medium_spark-optimized-p$p.log"
  hdfs dfs -rm -r -f "$out"

  run_job "Spark medium p$p" "$log" \
    /usr/bin/time -v spark-submit "$SPARK_SCRIPT" \
    "$INPUT_SPARK" "hdfs://namenode:9000$out" "$p" "$STOPWORDS_LOCAL"

  lines=$(hdfs dfs -cat "$out/part-*" 2>/dev/null | wc -l)
  echo "$out: $lines" | tee -a "$ANALYSIS_DIR/line_counts.txt"
done

echo "=== START SEQUENTIAL PYTHON MEDIUM ==="

SEQ_LOG="$LOG_DIR/medium_sequential.log"

run_job "Sequential Python medium" "$SEQ_LOG" \
  /usr/bin/time -v python3 "$SEQ_SCRIPT" \
  "$LOCAL_INPUT" "$SEQ_OUT"

seq_lines=$(wc -l < "$SEQ_OUT" 2>/dev/null || echo 0)
echo "sequential-medium: $seq_lines" | tee -a "$ANALYSIS_DIR/line_counts.txt"

echo "=== OUTPUT SIZES ===" | tee "$ANALYSIS_DIR/output_sizes.txt"
hdfs dfs -du -s -h "$OUT_BASE"/*/* 2>/dev/null | tee -a "$ANALYSIS_DIR/output_sizes.txt"
du -h "$SEQ_OUT" 2>/dev/null | tee -a "$ANALYSIS_DIR/output_sizes.txt"

echo "=== PERFORMANCE SUMMARY ===" | tee "$ANALYSIS_DIR/performance_summary.txt"
for log in "$LOG_DIR"/*.log; do
  [ -f "$log" ] || continue
  echo "--- $(basename "$log") ---" | tee -a "$ANALYSIS_DIR/performance_summary.txt"
  grep -E "Elapsed|Elapsed seconds|Maximum resident|User time|System time|Percent of CPU|Launched map tasks|Launched reduce tasks|Map input records|Map output records|Reduce input records|Reduce output records|Job Finished|Job failed|Exception|ERROR|Exit status" "$log" | tee -a "$ANALYSIS_DIR/performance_summary.txt"
done

echo "=== SAMPLE OUTPUTS ==="
mkdir -p "$ANALYSIS_DIR/samples"
hdfs dfs -cat "$OUT_BASE/medium/hadoop-inmapper-r1/part-*" 2>/dev/null | head -20 > "$ANALYSIS_DIR/samples/medium_hadoop_inmapper_sample.txt"
hdfs dfs -cat "$OUT_BASE/medium/spark-optimized-p1/part-*" 2>/dev/null | head -20 > "$ANALYSIS_DIR/samples/medium_spark_sample.txt"
head -20 "$SEQ_OUT" 2>/dev/null > "$ANALYSIS_DIR/samples/medium_sequential_sample.txt"

echo "=== FINAL SUMMARY ===" | tee "$ANALYSIS_DIR/final_summary.txt"
cat "$ANALYSIS_DIR/cluster_info.txt" | tee -a "$ANALYSIS_DIR/final_summary.txt"
cat "$ANALYSIS_DIR/dataset_info.txt" | tee -a "$ANALYSIS_DIR/final_summary.txt"
cat "$ANALYSIS_DIR/line_counts.txt" | tee -a "$ANALYSIS_DIR/final_summary.txt"
cat "$ANALYSIS_DIR/output_sizes.txt" | tee -a "$ANALYSIS_DIR/final_summary.txt"
cat "$ANALYSIS_DIR/performance_summary.txt" | tee -a "$ANALYSIS_DIR/final_summary.txt"

echo "=== CREATE SUMMARY ARCHIVE ==="
rm -rf "$SUMMARY_DIR"
mkdir -p "$SUMMARY_DIR/logs" "$SUMMARY_DIR/samples" "$SUMMARY_DIR/sequential_outputs"

cp "$ANALYSIS_DIR"/*.txt "$SUMMARY_DIR/" 2>/dev/null || true
cp "$ANALYSIS_DIR"/samples/* "$SUMMARY_DIR/samples/" 2>/dev/null || true
cp "$LOG_DIR"/*.log "$SUMMARY_DIR/logs/" 2>/dev/null || true
cp "$SEQ_OUT" "$SUMMARY_DIR/sequential_outputs/" 2>/dev/null || true

cat > "$SUMMARY_DIR/README.txt" << 'EOF'
Medium dataset experiment summary archive.
Contains Hadoop Base, Hadoop InMapper, Spark, and Sequential Python results.
Includes logs, line counts, output sizes, samples, and sequential output index.
EOF

tar -czf results/analysis/final_exp_medium_summary.tar.gz -C results/analysis final_exp_medium_summary

echo "=== DONE ==="
echo "Send me:"
echo "~/Cloud/results/analysis/final_exp_medium_summary.tar.gz"
ls -lh results/analysis/final_exp_medium_summary.tar.gz