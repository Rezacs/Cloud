#!/usr/bin/env bash
bash ~/Cloud/run_one_dataset.sh \
  large \
  "/input/gutenberg-large" \
  "hdfs:///input/gutenberg-large" \
  "/var/backups/hadoop/backup_before_reinstall/AllDatasets/large"