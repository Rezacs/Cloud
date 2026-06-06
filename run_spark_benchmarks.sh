#!/usr/bin/env bash
set -u

DATASET="news-small-1k"
INPUT_PATH="hdfs:///input/news-small-1k/*/*"
STOPWORDS="/home/hadoop/Cloud/hadoop-java/src/main/resources/stopwords.txt"
SCRIPT_PATH="/home/hadoop/Cloud/spark-python/spark_inverted_index_fastest.py"

echo "=========================================================="
echo " Starting Benchmarks for Dataset: $DATASET"
echo "=========================================================="

for p in 1 2 4 8 16 24
do
    OUT_PATH="/output/manual-test-p$p"
    
    # Clean old output directories in HDFS before running
    hadoop fs -rm -r -f "$OUT_PATH" > /dev/null 2>&1
    
    echo -n "Running with p = $p ... "
    
    # Run spark-submit inside /usr/bin/time, capturing only the wall clock line
    # redirecting standard spark logs completely to /dev/null
    /usr/bin/time -v spark-submit \
      --master yarn \
      --deploy-mode client \
      --num-executors 2 \
      --driver-memory 1g \
      --executor-memory 1500m \
      --executor-cores 2 \
      --conf spark.executor.memoryOverhead=384 \
      --conf spark.python.worker.reuse=true \
      "$SCRIPT_PATH" "$INPUT_PATH" "$OUT_PATH" "$p" "$STOPWORDS" 2>&1 \
      | grep "Elapsed (wall clock)"
      
done

echo "=========================================================="
echo " All Benchmarks Completed Successfully!"
echo "=========================================================="
