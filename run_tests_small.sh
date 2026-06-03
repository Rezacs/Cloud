#!/usr/bin/env bash
set -e

export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export HADOOP_HOME=/opt/hadoop
export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
export YARN_CONF_DIR=/opt/hadoop/etc/hadoop
export HADOOP_COMMON_HOME=/opt/hadoop
export HADOOP_MAPRED_HOME=/opt/hadoop
export HADOOP_HDFS_HOME=/opt/hadoop
export YARN_HOME=/opt/hadoop
export SPARK_HOME=/usr/local/spark
export PATH=$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH

cd ~/Cloud

REDUCERS="1 2 4 8 16 24"

JAR="$HOME/Cloud/hadoop-java/target/hadoop-inverted-index-1.0.jar"
SPARK_SCRIPT="$HOME/Cloud/spark-python/inverted_index_spark.py"
SEQ_SCRIPT="$HOME/Cloud/sequential-python/inverted_index_sequential.py"

STOPWORDS_HDFS="/stopwords.txt"
STOPWORDS_LOCAL="$HOME/Cloud/hadoop-java/src/main/resources/stopwords.txt"

BASE_OUT="/output/final-exp-small"
LOG_DIR="results/logs/final_exp_small"
ANALYSIS_DIR="results/analysis/final_exp_small"
SEQ_OUT_DIR="$ANALYSIS_DIR/sequential_outputs"

SIZE="small"
HDFS_INPUT="/input/small/*/*"
SPARK_INPUT="hdfs:///input/small/*/*"
LOCAL_INPUT="/var/backups/hadoop/backup_before_reinstall/datasets"

echo "=== CLEAN PREVIOUS SMALL RESULTS ==="
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
echo "--- /input/small ---" | tee -a "$ANALYSIS_DIR/dataset_info.txt"
hdfs dfs -du -s -h /input/small | tee -a "$ANALYSIS_DIR/dataset_info.txt"
hdfs dfs -count /input/small | tee -a "$ANALYSIS_DIR/dataset_info.txt"

echo "=== LOCAL DATASET CHECK ===" | tee -a "$ANALYSIS_DIR/dataset_info.txt"
echo "--- $LOCAL_INPUT ---" | tee -a "$ANALYSIS_DIR/dataset_info.txt"
[ -d "$LOCAL_INPUT" ] && echo "EXISTS" | tee -a "$ANALYSIS_DIR/dataset_info.txt" || echo "MISSING" | tee -a "$ANALYSIS_DIR/dataset_info.txt"
[ -d "$LOCAL_INPUT" ] && find "$LOCAL_INPUT" -type f -name "*.txt" | wc -l | tee -a "$ANALYSIS_DIR/dataset_info.txt"
[ -d "$LOCAL_INPUT" ] && du -sh "$LOCAL_INPUT" | tee -a "$ANALYSIS_DIR/dataset_info.txt"

echo "=== START HADOOP BASE EXPERIMENTS ==="

for r in $REDUCERS; do
echo "=== Hadoop BASE | dataset=$SIZE | reducers=$r ==="

```
out="$BASE_OUT/$SIZE/hadoop-base-r$r"
log="$LOG_DIR/${SIZE}_hadoop-base-r$r.log"

hdfs dfs -rm -r -f "$out"

start=$SECONDS

/usr/bin/time -v hadoop jar "$JAR" \
    it.unipi.cloud.InvertedIndex \
    "$HDFS_INPUT" \
    "$out" \
    "$r" \
    "$STOPWORDS_HDFS" \
    > "$log" 2>&1

exit_code=$?
elapsed=$((SECONDS - start))

echo "Finished Hadoop BASE $SIZE r$r, exit=$exit_code, seconds=${elapsed}s"
echo "Elapsed seconds: $elapsed" >> "$log"
```

done

echo "=== START HADOOP INMAPPER EXPERIMENTS ==="

for r in $REDUCERS; do
echo "=== Hadoop INMAPPER | dataset=$SIZE | reducers=$r ==="

```
out="$BASE_OUT/$SIZE/hadoop-inmapper-r$r"
log="$LOG_DIR/${SIZE}_hadoop-inmapper-r$r.log"

hdfs dfs -rm -r -f "$out"

start=$SECONDS

/usr/bin/time -v hadoop jar "$JAR" \
    it.unipi.cloud.InvertedIndexInMapper \
    "$HDFS_INPUT" \
    "$out" \
    "$r" \
    "$STOPWORDS_HDFS" \
    > "$log" 2>&1

exit_code=$?
elapsed=$((SECONDS - start))

echo "Finished Hadoop INMAPPER $SIZE r$r, exit=$exit_code, seconds=${elapsed}s"
echo "Elapsed seconds: $elapsed" >> "$log"
```

done

echo "=== START SPARK EXPERIMENTS ==="

for p in $REDUCERS; do
echo "=== Spark OPTIMIZED | dataset=$SIZE | partitions=$p ==="

```
out="$BASE_OUT/$SIZE/spark-optimized-p$p"
log="$LOG_DIR/${SIZE}_spark-optimized-p$p.log"

hdfs dfs -rm -r -f "$out"

start=$SECONDS

/usr/bin/time -v spark-submit "$SPARK_SCRIPT" \
    "$SPARK_INPUT" \
    "hdfs://namenode:9000$out" \
    "$p" \
    "$STOPWORDS_LOCAL" \
    > "$log" 2>&1

exit_code=$?
elapsed=$((SECONDS - start))

echo "Finished Spark OPTIMIZED $SIZE p$p, exit=$exit_code, seconds=${elapsed}s"
echo "Elapsed seconds: $elapsed" >> "$log"
```

done

echo "=== START SEQUENTIAL PYTHON EXPERIMENT ===" | tee "$ANALYSIS_DIR/sequential_summary.txt"

seq_file="$SEQ_OUT_DIR/index_small.txt"
seq_log="$LOG_DIR/small_sequential.log"

start=$SECONDS

/usr/bin/time -v python3 "$SEQ_SCRIPT" 
"$LOCAL_INPUT" 
"$seq_file" 
> "$ANALYSIS_DIR/small_sequential_stdout.txt" 2> "$seq_log"

exit_code=$?
elapsed=$((SECONDS - start))

echo "Finished Sequential Python $SIZE, exit=$exit_code, seconds=${elapsed}s"
echo "Elapsed seconds: $elapsed" >> "$seq_log"

echo "exit_code=$exit_code" | tee -a "$ANALYSIS_DIR/sequential_summary.txt"
echo "index_terms=$(wc -l < "$seq_file" 2>/dev/null || echo 0)" | tee -a "$ANALYSIS_DIR/sequential_summary.txt"
echo "output_size=$(du -h "$seq_file" 2>/dev/null | awk '{print $1}')" | tee -a "$ANALYSIS_DIR/sequential_summary.txt"
grep -E "Elapsed|Elapsed seconds|Maximum resident|User time|System time|Percent of CPU" "$seq_log" | tee -a "$ANALYSIS_DIR/sequential_summary.txt"

echo "=== PERFORMANCE SUMMARY ===" | tee "$ANALYSIS_DIR/performance_summary.txt"

for log in "$LOG_DIR"/*.log; do
[ -f "$log" ] || continue
name="$(basename "$log")"

```
if echo "$name" | grep -q "sequential"; then
    continue
fi

echo "--- $name ---" | tee -a "$ANALYSIS_DIR/performance_summary.txt"

grep -E "Elapsed|Elapsed seconds|Maximum resident|User time|System time|Percent of CPU|CPU time spent|Physical memory|Virtual memory|Total time spent|Launched map tasks|Launched reduce tasks|Map input records|Map output records|Reduce input records|Reduce output records|FILE: Number of bytes|HDFS: Number of bytes|Job Finished|Job failed|Exception|ERROR" "$log" | tee -a "$ANALYSIS_DIR/performance_summary.txt"
```

done

echo "=== LINE COUNTS ===" | tee "$ANALYSIS_DIR/line_counts.txt"

for impl in hadoop-base hadoop-inmapper; do
for r in $REDUCERS; do
path="$BASE_OUT/$SIZE/$impl-r$r"
echo -n "$path: " | tee -a "$ANALYSIS_DIR/line_counts.txt"
hdfs dfs -cat "$path/part-*" 2>/dev/null | wc -l | tee -a "$ANALYSIS_DIR/line_counts.txt"
done
done

for p in $REDUCERS; do
path="$BASE_OUT/$SIZE/spark-optimized-p$p"
echo -n "$path: " | tee -a "$ANALYSIS_DIR/line_counts.txt"
hdfs dfs -cat "$path/part-*" 2>/dev/null | wc -l | tee -a "$ANALYSIS_DIR/line_counts.txt"
done

echo -n "sequential-small: " | tee -a "$ANALYSIS_DIR/line_counts.txt"
wc -l < "$seq_file" 2>/dev/null | tee -a "$ANALYSIS_DIR/line_counts.txt"

echo "=== OUTPUT SIZES ===" | tee "$ANALYSIS_DIR/output_sizes.txt"
hdfs dfs -du -s -h "$BASE_OUT"/*/* 2>/dev/null | tee -a "$ANALYSIS_DIR/output_sizes.txt"
du -h "$SEQ_OUT_DIR"/index_*.txt 2>/dev/null | tee -a "$ANALYSIS_DIR/output_sizes.txt"

echo "=== SAMPLE OUTPUTS ==="
mkdir -p "$ANALYSIS_DIR/samples"

hdfs dfs -cat "$BASE_OUT/$SIZE/hadoop-inmapper-r1/part-*" 2>/dev/null | head -20 > "$ANALYSIS_DIR/samples/small_hadoop_inmapper_sample.txt"
hdfs dfs -cat "$BASE_OUT/$SIZE/spark-optimized-p1/part-*" 2>/dev/null | head -20 > "$ANALYSIS_DIR/samples/small_spark_sample.txt"
head -20 "$seq_file" 2>/dev/null > "$ANALYSIS_DIR/samples/small_sequential_sample.txt"

echo "=== FINAL SUMMARY ===" | tee "$ANALYSIS_DIR/final_summary.txt"
cat "$ANALYSIS_DIR/cluster_info.txt" | tee -a "$ANALYSIS_DIR/final_summary.txt"
cat "$ANALYSIS_DIR/dataset_info.txt" | tee -a "$ANALYSIS_DIR/final_summary.txt"
cat "$ANALYSIS_DIR/line_counts.txt" | tee -a "$ANALYSIS_DIR/final_summary.txt"
cat "$ANALYSIS_DIR/output_sizes.txt" | tee -a "$ANALYSIS_DIR/final_summary.txt"
cat "$ANALYSIS_DIR/sequential_summary.txt" | tee -a "$ANALYSIS_DIR/final_summary.txt"
cat "$ANALYSIS_DIR/performance_summary.txt" | tee -a "$ANALYSIS_DIR/final_summary.txt"

echo "=== CREATE SUMMARY ARCHIVE ==="

SUMMARY_DIR="results/analysis/final_exp_small_summary"

rm -rf "$SUMMARY_DIR"
mkdir -p "$SUMMARY_DIR/samples" "$SUMMARY_DIR/logs"

cp "$ANALYSIS_DIR"/*.txt "$SUMMARY_DIR"/ 2>/dev/null || true
cp "$ANALYSIS_DIR"/samples/* "$SUMMARY_DIR/samples/" 2>/dev/null || true
cp "$LOG_DIR"/*.log "$SUMMARY_DIR/logs/" 2>/dev/null || true

cat > "$SUMMARY_DIR/README.txt" << 'EOF'
Small dataset experiment summary archive.
Contains summary metrics, line counts, output sizes, samples, and logs.
EOF

tar -czf results/analysis/final_exp_small_summary.tar.gz -C results/analysis final_exp_small_summary

echo "=== DONE ==="
echo "Send me:"
echo "~/Cloud/results/analysis/final_exp_small_summary.tar.gz"
ls -lh results/analysis/final_exp_small_summary.tar.gz
