#!/usr/bin/env bash
set -euo pipefail
SPARK_HOME=/opt/spark-3.5.3-bin-hadoop3
HADOOP_HOME=/opt/hadoop-3.4.1
[[ "$(hostname -s)" == team-22-en ]] || { echo 'Run on edge'; exit 1; }
[[ ! -L "$SPARK_HOME" && -f "$SPARK_HOME/.spark-hw4-owned" ]] || { echo 'Owned Spark installation not found'; exit 1; }
echo 'Remove the HW4 Spark installation and its jars archive in HDFS. Hive tables hw4.*, input data, HDFS, YARN and Hive are preserved.'
read -r -p 'Type REMOVE-SPARK to continue: ' answer
[[ "$answer" == REMOVE-SPARK ]] || { echo 'Cancelled'; exit 1; }
"$HADOOP_HOME/bin/hdfs" dfs -rm -f -skipTrash /user/team/spark-hw4/spark-libs.jar
[[ "$(readlink -f /opt/spark)" != "$SPARK_HOME" ]] || sudo rm -f -- /opt/spark
[[ "$(readlink -f "$SPARK_HOME")" == /opt/spark-3.5.3-bin-hadoop3 ]]
sudo rm -rf -- /opt/spark-3.5.3-bin-hadoop3
echo 'Spark removed. Tables and data retained; ./deploy.sh restores it.'
