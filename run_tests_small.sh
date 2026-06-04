#!/usr/bin/env bash
bash ~/Cloud/run_one_dataset.sh \
  small \
  "/input/small/*/*" \
  "hdfs:///input/small/*/*" \
  "/var/backups/hadoop/backup_before_reinstall/AllDatasets/Small"