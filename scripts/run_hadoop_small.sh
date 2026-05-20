#!/bin/bash
set -e

hdfs dfs -rm -r -f /output/hadoop-index-small

/usr/bin/time -v hadoop jar ~/Cloud/hadoop-java/target/hadoop-inverted-index-1.0.jar \
it.unipi.cloud.InvertedIndex \
/input/full_dataset/* \
/output/hadoop-index-small