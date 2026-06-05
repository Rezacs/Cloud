echo "===== LOCAL DISK SPACE ON EACH NODE ====="
for node in namenode datanode2 datanode3; do
  echo "===== $node ====="
  ssh hadoop@$node "df -h / | awk 'NR==1 || NR==2 {print}'"
done

echo "===== HDFS SPACE ====="
hdfs dfsadmin -report | grep -E "Configured Capacity|Present Capacity|DFS Remaining|DFS Used|DFS Used%|Live datanodes"

echo "===== CURRENT HDFS INPUT SIZES ====="
hdfs dfs -du -s -h /input/* 2>/dev/null | sort -h

echo "===== LOCAL archive_1gb DATASET ====="
du -sh /var/backups/hadoop/backup_before_reinstall/AllDatasets/archive_1gb
find /var/backups/hadoop/backup_before_reinstall/AllDatasets/archive_1gb -type f | wc -l