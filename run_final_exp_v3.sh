cd ~/Cloud

REDUCERS="1 2 4 8 16 24"

JAR="$HOME/Cloud/hadoop-java/target/hadoop-inverted-index-1.0.jar"
SPARK_SCRIPT="$HOME/Cloud/spark-python/inverted_index_spark.py"
SEQ_SCRIPT="$HOME/Cloud/sequential-python/inverted_index_sequential.py"

STOPWORDS_HDFS="/stopwords.txt"
STOPWORDS_LOCAL="$HOME/Cloud/hadoop-java/src/main/resources/stopwords.txt"

BASE_OUT="/output/final-exp-v3"
LOG_DIR="results/logs/final_exp_v3"
ANALYSIS_DIR="results/analysis/final_exp_v3"
SEQ_OUT_DIR="$ANALYSIS_DIR/sequential_outputs"

declare -A HDFS_INPUTS
HDFS_INPUTS[small]="/input/small/*/*"
HDFS_INPUTS[medium]="/input/gutenberg-medium"
HDFS_INPUTS[large]="/input/gutenberg-large"

declare -A SPARK_INPUTS
SPARK_INPUTS[small]="hdfs:///input/small/*/*"
SPARK_INPUTS[medium]="hdfs:///input/gutenberg-medium"
SPARK_INPUTS[large]="hdfs:///input/gutenberg-large"

declare -A LOCAL_INPUTS
LOCAL_INPUTS[small]="/var/backups/hadoop/backup_before_reinstall/datasets"
LOCAL_INPUTS[medium]="/var/backups/hadoop/backup_before_reinstall/gutenberg/medium"
LOCAL_INPUTS[large]="/var/backups/hadoop/backup_before_reinstall/gutenberg/large"

echo "=== CLEAN PREVIOUS V3 RESULTS ==="
hdfs dfs -rm -r -f "$BASE_OUT"
rm -rf "$LOG_DIR" "$ANALYSIS_DIR"
mkdir -p "$LOG_DIR" "$ANALYSIS_DIR" "$SEQ_OUT_DIR"

echo "=== CHECK CLUSTER ===" | tee "$ANALYSIS_DIR/cluster_info.txt"
yarn node -list -all | tee -a "$ANALYSIS_DIR/cluster_info.txt"
hdfs dfsadmin -report | grep -E "Live datanodes|Name:|Hostname|DFS Used|DFS Remaining" | tee -a "$ANALYSIS_DIR/cluster_info.txt"

echo "=== BUILD HADOOP PROJECT ==="
cd ~/Cloud/hadoop-java
mvn clean package
cd ~/Cloud

echo "=== UPLOAD STOPWORDS ==="
hdfs dfs -put -f "$STOPWORDS_LOCAL" "$STOPWORDS_HDFS"

echo "=== DATASET CHECK ===" | tee "$ANALYSIS_DIR/dataset_info.txt"
for p in /input/small /input/gutenberg-medium /input/gutenberg-large; do
  echo "--- $p ---" | tee -a "$ANALYSIS_DIR/dataset_info.txt"
  hdfs dfs -du -s -h "$p" | tee -a "$ANALYSIS_DIR/dataset_info.txt"
  hdfs dfs -count "$p" | tee -a "$ANALYSIS_DIR/dataset_info.txt"
done

echo "=== LOCAL DATASET CHECK ===" | tee -a "$ANALYSIS_DIR/dataset_info.txt"
for p in \
  "/var/backups/hadoop/backup_before_reinstall/datasets" \
  "/var/backups/hadoop/backup_before_reinstall/gutenberg/medium" \
  "/var/backups/hadoop/backup_before_reinstall/gutenberg/large"
do
  echo "--- $p ---" | tee -a "$ANALYSIS_DIR/dataset_info.txt"
  [ -d "$p" ] && echo "EXISTS" | tee -a "$ANALYSIS_DIR/dataset_info.txt" || echo "MISSING" | tee -a "$ANALYSIS_DIR/dataset_info.txt"
  [ -d "$p" ] && find "$p" -type f -name "*.txt" | wc -l | tee -a "$ANALYSIS_DIR/dataset_info.txt"
  [ -d "$p" ] && du -sh "$p" | tee -a "$ANALYSIS_DIR/dataset_info.txt"
done

echo "=== START DISTRIBUTED EXPERIMENTS ==="

for size in small medium large; do
  hdfs_input="${HDFS_INPUTS[$size]}"
  spark_input="${SPARK_INPUTS[$size]}"

  for r in $REDUCERS; do
    echo "=== Hadoop BASE | dataset=$size | reducers=$r ==="
    out="$BASE_OUT/$size/hadoop-base-r$r"
    log="$LOG_DIR/${size}_hadoop-base-r$r.log"
    hdfs dfs -rm -r -f "$out"

    /usr/bin/time -v hadoop jar "$JAR" \
      it.unipi.cloud.InvertedIndex \
      "$hdfs_input" \
      "$out" \
      "$r" \
      "$STOPWORDS_HDFS" \
      > "$log" 2>&1

    echo "Finished Hadoop BASE $size r$r, exit=$?"
  done

  for r in $REDUCERS; do
    echo "=== Hadoop INMAPPER | dataset=$size | reducers=$r ==="
    out="$BASE_OUT/$size/hadoop-inmapper-r$r"
    log="$LOG_DIR/${size}_hadoop-inmapper-r$r.log"
    hdfs dfs -rm -r -f "$out"

    /usr/bin/time -v hadoop jar "$JAR" \
      it.unipi.cloud.InvertedIndexInMapper \
      "$hdfs_input" \
      "$out" \
      "$r" \
      "$STOPWORDS_HDFS" \
      > "$log" 2>&1

    echo "Finished Hadoop INMAPPER $size r$r, exit=$?"
  done

  for p in $REDUCERS; do
    echo "=== Spark OPTIMIZED | dataset=$size | partitions=$p ==="
    out="$BASE_OUT/$size/spark-optimized-p$p"
    log="$LOG_DIR/${size}_spark-optimized-p$p.log"
    hdfs dfs -rm -r -f "$out"

    /usr/bin/time -v spark-submit "$SPARK_SCRIPT" \
      "$spark_input" \
      "hdfs://namenode:9000$out" \
      "$p" \
      "$STOPWORDS_LOCAL" \
      > "$log" 2>&1

    echo "Finished Spark OPTIMIZED $size p$p, exit=$?"
  done
done

echo "=== START SEQUENTIAL PYTHON EXPERIMENTS ===" | tee "$ANALYSIS_DIR/sequential_summary.txt"

for size in small medium large; do
  input="${LOCAL_INPUTS[$size]}"
  output="$SEQ_OUT_DIR/index_${size}.txt"
  log="$LOG_DIR/${size}_sequential.log"

  echo "=== Sequential Python | dataset=$size ===" | tee -a "$ANALYSIS_DIR/sequential_summary.txt"
  echo "input=$input" | tee -a "$ANALYSIS_DIR/sequential_summary.txt"

  /usr/bin/time -v python3 "$SEQ_SCRIPT" \
    "$input" \
    "$output" \
    > "$ANALYSIS_DIR/${size}_sequential_stdout.txt" 2> "$log"

  echo "exit_code=$?" | tee -a "$ANALYSIS_DIR/sequential_summary.txt"
  echo "index_terms=$(wc -l < "$output" 2>/dev/null || echo 0)" | tee -a "$ANALYSIS_DIR/sequential_summary.txt"
  echo "output_size=$(du -h "$output" 2>/dev/null | awk '{print $1}')" | tee -a "$ANALYSIS_DIR/sequential_summary.txt"
  grep -E "Elapsed|Maximum resident|User time|System time|Percent of CPU" "$log" | tee -a "$ANALYSIS_DIR/sequential_summary.txt"
done

echo "=== PERFORMANCE SUMMARY ===" | tee "$ANALYSIS_DIR/performance_summary.txt"
for log in "$LOG_DIR"/*.log; do
  [ -f "$log" ] || continue
  name="$(basename "$log")"

  if echo "$name" | grep -q "sequential"; then
    continue
  fi

  echo "--- $name ---" | tee -a "$ANALYSIS_DIR/performance_summary.txt"
  grep -E "Elapsed|Maximum resident|User time|System time|Percent of CPU|CPU time spent|Physical memory|Virtual memory|Total time spent|Launched map tasks|Launched reduce tasks|Map input records|Map output records|Reduce input records|Reduce output records|FILE: Number of bytes|HDFS: Number of bytes|Job Finished|Job failed|Exception|ERROR" "$log" | tee -a "$ANALYSIS_DIR/performance_summary.txt"
done

echo "=== LINE COUNTS ===" | tee "$ANALYSIS_DIR/line_counts.txt"
for size in small medium large; do
  for impl in hadoop-base hadoop-inmapper; do
    for r in $REDUCERS; do
      path="$BASE_OUT/$size/$impl-r$r"
      echo -n "$path: " | tee -a "$ANALYSIS_DIR/line_counts.txt"
      hdfs dfs -cat "$path/part-*" 2>/dev/null | wc -l | tee -a "$ANALYSIS_DIR/line_counts.txt"
    done
  done

  for p in $REDUCERS; do
    path="$BASE_OUT/$size/spark-optimized-p$p"
    echo -n "$path: " | tee -a "$ANALYSIS_DIR/line_counts.txt"
    hdfs dfs -cat "$path/part-*" 2>/dev/null | wc -l | tee -a "$ANALYSIS_DIR/line_counts.txt"
  done

  seq_file="$SEQ_OUT_DIR/index_${size}.txt"
  echo -n "sequential-$size: " | tee -a "$ANALYSIS_DIR/line_counts.txt"
  wc -l < "$seq_file" 2>/dev/null | tee -a "$ANALYSIS_DIR/line_counts.txt"
done

echo "=== OUTPUT SIZES ===" | tee "$ANALYSIS_DIR/output_sizes.txt"
hdfs dfs -du -s -h "$BASE_OUT"/*/* 2>/dev/null | tee -a "$ANALYSIS_DIR/output_sizes.txt"
du -h "$SEQ_OUT_DIR"/index_*.txt 2>/dev/null | tee -a "$ANALYSIS_DIR/output_sizes.txt"

echo "=== SAMPLE OUTPUTS ==="
mkdir -p "$ANALYSIS_DIR/samples"
for size in small medium large; do
  hdfs dfs -cat "$BASE_OUT/$size/hadoop-inmapper-r1/part-*" 2>/dev/null | head -20 > "$ANALYSIS_DIR/samples/${size}_hadoop_inmapper_sample.txt"
  hdfs dfs -cat "$BASE_OUT/$size/spark-optimized-p1/part-*" 2>/dev/null | head -20 > "$ANALYSIS_DIR/samples/${size}_spark_sample.txt"
  head -20 "$SEQ_OUT_DIR/index_${size}.txt" 2>/dev/null > "$ANALYSIS_DIR/samples/${size}_sequential_sample.txt"
done

echo "=== FINAL SUMMARY ===" | tee "$ANALYSIS_DIR/final_summary.txt"
cat "$ANALYSIS_DIR/cluster_info.txt" | tee -a "$ANALYSIS_DIR/final_summary.txt"
cat "$ANALYSIS_DIR/dataset_info.txt" | tee -a "$ANALYSIS_DIR/final_summary.txt"
cat "$ANALYSIS_DIR/line_counts.txt" | tee -a "$ANALYSIS_DIR/final_summary.txt"
cat "$ANALYSIS_DIR/output_sizes.txt" | tee -a "$ANALYSIS_DIR/final_summary.txt"
cat "$ANALYSIS_DIR/sequential_summary.txt" | tee -a "$ANALYSIS_DIR/final_summary.txt"
cat "$ANALYSIS_DIR/performance_summary.txt" | tee -a "$ANALYSIS_DIR/final_summary.txt"

echo "=== CREATE SUMMARY ARCHIVE WITHOUT FULL SEQUENTIAL INDEX FILES ==="
SUMMARY_DIR="results/analysis/final_exp_v3_summary"
rm -rf "$SUMMARY_DIR"
mkdir -p "$SUMMARY_DIR/samples" "$SUMMARY_DIR/logs"

cp "$ANALYSIS_DIR"/cluster_info.txt "$SUMMARY_DIR"/ 2>/dev/null || true
cp "$ANALYSIS_DIR"/dataset_info.txt "$SUMMARY_DIR"/ 2>/dev/null || true
cp "$ANALYSIS_DIR"/line_counts.txt "$SUMMARY_DIR"/ 2>/dev/null || true
cp "$ANALYSIS_DIR"/output_sizes.txt "$SUMMARY_DIR"/ 2>/dev/null || true
cp "$ANALYSIS_DIR"/performance_summary.txt "$SUMMARY_DIR"/ 2>/dev/null || true
cp "$ANALYSIS_DIR"/sequential_summary.txt "$SUMMARY_DIR"/ 2>/dev/null || true
cp "$ANALYSIS_DIR"/final_summary.txt "$SUMMARY_DIR"/ 2>/dev/null || true
cp "$ANALYSIS_DIR"/samples/* "$SUMMARY_DIR/samples/" 2>/dev/null || true
cp "$LOG_DIR"/*_sequential.log "$SUMMARY_DIR/logs/" 2>/dev/null || true

cat > "$SUMMARY_DIR/README.txt" << 'EOF'
Final experiment v3 summary archive.
Contains all summary metrics, line counts, output sizes, samples, and sequential logs.
Full sequential index output files are excluded because they are large.
EOF

tar -czf results/analysis/final_exp_v3_summary.tar.gz -C results/analysis final_exp_v3_summary

echo "=== DONE ==="
echo "Send me:"
echo "~/Cloud/results/analysis/final_exp_v3_summary.tar.gz"
ls -lh results/analysis/final_exp_v3_summary.tar.gz