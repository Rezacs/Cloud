#!/bin/bash
set -e

hdfs dfs -rm -r -f /output/spark-index-small

/usr/bin/time -v spark-submit \
~/Cloud/spark-python/inverted_index_spark.py \
hdfs:///input/full_dataset/* \
hdfs:///output/spark-index-small