#!/bin/bash
# Creates the three data lake zones once and gives them to the 'pipeline' user.
# Other users cannot read them (permission 750).
set -euo pipefail

hdfs dfsadmin -safemode wait
for zone in raw processed curated; do
  hdfs dfs -mkdir -p "/bgl/$zone"
done
hdfs dfs -chown -R pipeline:pipeline /bgl
hdfs dfs -chmod -R 750 /bgl
hdfs dfs -ls /bgl
echo "HDFS zones are ready."
