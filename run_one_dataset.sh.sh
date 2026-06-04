#!/usr/bin/env bash
set -u

DATASET="$1"
INPUT_HADOOP="$2"
INPUT_SPARK="$3"
LOCAL_INPUT="$4"

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

OUT_BASE="/output/final-exp-$DATASET"
LOG_DIR="results/logs/final_exp_$DATASET"
ANALYSIS_DIR="results/analysis/final_exp_$DATASET"
SUMMARY_DIR="results/analysis/final_exp_${DATASET}_summary"
SEQ_OUT="$ANALYSIS_DIR/sequential_outputs/index_${DATASET}.txt"
SEQ_LOG="$LOG_DIR/${DATASET}_sequential.log"

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

echo "=== CLEAN $DATASET ==="
hdfs dfs -rm -r -f "$OUT_BASE"
rm -rf "$LOG_DIR" "$ANALYSIS_DIR" "$SUMMARY_DIR"
mkdir -p "$LOG_DIR" "$ANALYSIS_DIR" "$(dirname "$SEQ_OUT")"

echo "=== BUILD ==="
cd ~/Cloud/hadoop-java && mvn clean package && cd ~/Cloud

echo "=== UPLOAD STOPWORDS ==="
hdfs dfs -put -f "$STOPWORDS_LOCAL" "$STOPWORDS_HDFS"

echo "=== DATASET CHECK ===" | tee "$ANALYSIS_DIR/dataset_info.txt"
echo "HDFS Hadoop input: $INPUT_HADOOP" | tee -a "$ANALYSIS_DIR/dataset_info.txt"
echo "HDFS Spark input: $INPUT_SPARK" | tee -a "$ANALYSIS_DIR/dataset_info.txt"
echo "Local sequential input: $LOCAL_INPUT" | tee -a "$ANALYSIS_DIR/dataset_info.txt"
hdfs dfs -count "${INPUT_HADOOP%%/*/*}" 2>/dev/null | tee -a "$ANALYSIS_DIR/dataset_info.txt" || true
du -sh "$LOCAL_INPUT" 2>/dev/null | tee -a "$ANALYSIS_DIR/dataset_info.txt" || true
find "$LOCAL_INPUT" -type f -name "*.txt" 2>/dev/null | wc -l | tee -a "$ANALYSIS_DIR/dataset_info.txt"

echo "=== HADOOP BASE ==="
for r in $REDUCERS; do
  out="$OUT_BASE/$DATASET/hadoop-base-r$r"
  log="$LOG_DIR/${DATASET}_hadoop-base-r$r.log"
  hdfs dfs -rm -r -f "$out"

  run_job "Hadoop BASE $DATASET r$r" "$log" \
    /usr/bin/time -v hadoop jar "$JAR" \
    it.unipi.cloud.InvertedIndex \
    "$INPUT_HADOOP" "$out" "$r" "$STOPWORDS_HDFS"

  echo -n "lines: "
  hdfs dfs -cat "$out/part-*" 2>/dev/null | wc -l
done

echo "=== HADOOP INMAPPER ==="
for r in $REDUCERS; do
  out="$OUT_BASE/$DATASET/hadoop-inmapper-r$r"
  log="$LOG_DIR/${DATASET}_hadoop-inmapper-r$r.log"
  hdfs dfs -rm -r -f "$out"

  run_job "Hadoop INMAPPER $DATASET r$r" "$log" \
    /usr/bin/time -v hadoop jar "$JAR" \
    it.unipi.cloud.InvertedIndexInMapper \
    "$INPUT_HADOOP" "$out" "$r" "$STOPWORDS_HDFS"

  echo -n "lines: "
  hdfs dfs -cat "$out/part-*" 2>/dev/null | wc -l
done

echo "=== SPARK ==="
for p in $REDUCERS; do
  out="$OUT_BASE/$DATASET/spark-optimized-p$p"
  log="$LOG_DIR/${DATASET}_spark-optimized-p$p.log"
  hdfs dfs -rm -r -f "$out"

  run_job "Spark $DATASET p$p" "$log" \
    /usr/bin/time -v spark-submit \
    --master yarn \
    --deploy-mode client \
    --driver-memory 1g \
    --executor-memory 1g \
    --executor-cores 2 \
    "$SPARK_SCRIPT" \
    "$INPUT_SPARK" "hdfs://namenode:9000$out" "$p" "$STOPWORDS_LOCAL"

  echo -n "lines: "
  hdfs dfs -cat "$out/part-*" 2>/dev/null | wc -l
done

echo "=== SEQUENTIAL PYTHON ==="
run_job "Sequential Python $DATASET" "$SEQ_LOG" \
  /usr/bin/time -v python3 "$SEQ_SCRIPT" \
  "$LOCAL_INPUT" "$SEQ_OUT"

echo -n "sequential lines: "
wc -l < "$SEQ_OUT"

echo "=== CREATE LIGHT SUMMARY ARCHIVE ==="
rm -rf "$SUMMARY_DIR"
mkdir -p "$SUMMARY_DIR/logs" "$SUMMARY_DIR/samples"

cp "$LOG_DIR"/*.log "$SUMMARY_DIR/logs/" 2>/dev/null || true
cp "$ANALYSIS_DIR/dataset_info.txt" "$SUMMARY_DIR/" 2>/dev/null || true

rm -f "$SUMMARY_DIR/line_counts.txt"

for path in "$OUT_BASE/$DATASET"/*; do
  echo -n "$path: " >> "$SUMMARY_DIR/line_counts.txt"
  hdfs dfs -cat "$path/part-*" 2>/dev/null | wc -l >> "$SUMMARY_DIR/line_counts.txt"
done

echo -n "sequential-$DATASET: " >> "$SUMMARY_DIR/line_counts.txt"
wc -l < "$SEQ_OUT" >> "$SUMMARY_DIR/line_counts.txt"

hdfs dfs -du -s -h "$OUT_BASE/$DATASET"/* > "$SUMMARY_DIR/output_sizes.txt" 2>/dev/null
du -h "$SEQ_OUT" >> "$SUMMARY_DIR/output_sizes.txt" 2>/dev/null

cat > "$SUMMARY_DIR/sequential_summary.txt" <<EOF
Sequential Python $DATASET dataset
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

hdfs dfs -cat "$OUT_BASE/$DATASET/hadoop-inmapper-r1/part-*" 2>/dev/null | head -20 > "$SUMMARY_DIR/samples/${DATASET}_hadoop_inmapper_sample.txt"
hdfs dfs -cat "$OUT_BASE/$DATASET/spark-optimized-p1/part-*" 2>/dev/null | head -20 > "$SUMMARY_DIR/samples/${DATASET}_spark_sample.txt"
head -20 "$SEQ_OUT" > "$SUMMARY_DIR/samples/${DATASET}_sequential_sample.txt"

cat > "$SUMMARY_DIR/README.txt" <<EOF
Light summary archive for $DATASET dataset.
Includes Hadoop Base, Hadoop InMapper, Spark, and Sequential Python.
Full sequential index is excluded.
EOF

tar -czf "results/analysis/final_exp_${DATASET}_summary_light.tar.gz" \
  -C results/analysis "final_exp_${DATASET}_summary"

ls -lh "results/analysis/final_exp_${DATASET}_summary_light.tar.gz"

echo "=== DONE $DATASET ==="