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
STOPWORDS_HDFS="/stopwords.txt"
STOPWORDS_LOCAL="$HOME/Cloud/hadoop-java/src/main/resources/stopwords.txt"

INPUT_HADOOP="/input/small/*/*"
INPUT_SPARK="hdfs:///input/small/*/*"

OUT_BASE="/output/final-exp-small"
LOG_DIR="results/logs/final_exp_small"
mkdir -p "$LOG_DIR"

echo "=== CLEAN ==="
hdfs dfs -rm -r -f "$OUT_BASE"

echo "=== BUILD ==="
cd ~/Cloud/hadoop-java && mvn clean package && cd ~/Cloud

echo "=== UPLOAD STOPWORDS ==="
hdfs dfs -put -f "$STOPWORDS_LOCAL" "$STOPWORDS_HDFS"

echo "=== DATASET CHECK ==="
hdfs dfs -count /input/small
hdfs dfs -du -s -h /input/small

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

  if [ "$code" -ne 0 ]; then
    echo "FAILED. Last log lines:"
    tail -n 40 "$log"
    exit "$code"
  fi
}

for r in $REDUCERS; do
  out="$OUT_BASE/small/hadoop-base-r$r"
  log="$LOG_DIR/small_hadoop-base-r$r.log"
  hdfs dfs -rm -r -f "$out"

  run_job "Hadoop BASE small r$r" "$log" \
    /usr/bin/time -v hadoop jar "$JAR" \
    it.unipi.cloud.InvertedIndex \
    "$INPUT_HADOOP" "$out" "$r" "$STOPWORDS_HDFS"

  echo -n "lines: "
  hdfs dfs -cat "$out/part-*" 2>/dev/null | wc -l
done

for r in $REDUCERS; do
  out="$OUT_BASE/small/hadoop-inmapper-r$r"
  log="$LOG_DIR/small_hadoop-inmapper-r$r.log"
  hdfs dfs -rm -r -f "$out"

  run_job "Hadoop INMAPPER small r$r" "$log" \
    /usr/bin/time -v hadoop jar "$JAR" \
    it.unipi.cloud.InvertedIndexInMapper \
    "$INPUT_HADOOP" "$out" "$r" "$STOPWORDS_HDFS"

  echo -n "lines: "
  hdfs dfs -cat "$out/part-*" 2>/dev/null | wc -l
done

for p in $REDUCERS; do
  out="$OUT_BASE/small/spark-optimized-p$p"
  log="$LOG_DIR/small_spark-optimized-p$p.log"
  hdfs dfs -rm -r -f "$out"

  run_job "Spark small p$p" "$log" \
    /usr/bin/time -v spark-submit "$SPARK_SCRIPT" \
    "$INPUT_SPARK" "hdfs://namenode:9000$out" "$p" "$STOPWORDS_LOCAL"

  echo -n "lines: "
  hdfs dfs -cat "$out/part-*" 2>/dev/null | wc -l
done

SEQ_SCRIPT="$HOME/Cloud/sequential-python/inverted_index_sequential.py"
LOCAL_INPUT="/var/backups/hadoop/backup_before_reinstall/datasets"
SEQ_OUT="results/analysis/final_exp_small/sequential_outputs/index_small.txt"
SEQ_LOG="$LOG_DIR/small_sequential.log"

mkdir -p "$(dirname "$SEQ_OUT")"

run_job "Sequential Python small" "$SEQ_LOG" \
  /usr/bin/time -v python3 "$SEQ_SCRIPT" \
  "$LOCAL_INPUT" "$SEQ_OUT"

echo -n "lines: "
wc -l < "$SEQ_OUT"

echo "=== DONE ==="