#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export HADOOP_HOME=/opt/hadoop-3.4.1
export HADOOP_CONF_DIR="$HADOOP_HOME/etc/hadoop"
export SPARK_HOME=/opt/spark-3.5.3-bin-hadoop3
export HIVE_HOME=/opt/apache-hive-4.0.0-alpha-2-bin
BEELINE="$HIVE_HOME/bin/beeline"
JDBC='jdbc:hive2://127.0.0.1:10000/default'
NN=hdfs://10.22.0.11:9000
HDFS_BASE=/user/team/spark-hw4

die() { printf 'ERROR: %s\n' "$*" >&2; exit 1; }

[[ -x "$SPARK_HOME/bin/spark-submit" ]] || die 'Spark is not installed: run ./deploy.sh'
[[ -x "$BEELINE" ]] || die 'Missing beeline (run HW3 first).'
[[ "$ROOT_DIR" != *\'* ]] || die 'The checkout path may not contain an apostrophe.'

shopt -s nullglob
files=("$ROOT_DIR"/data/events_*.csv)
(( ${#files[@]} > 0 )) || die 'No data/events_YYYY-MM-DD.csv files.'

echo '==> Validating input files'
for path in "${files[@]}"; do
  filename="${path##*/}"
  [[ "$filename" =~ ^events_([0-9]{4}-[0-9]{2}-[0-9]{2})\.csv$ ]] || die "Invalid filename: $filename"
  part_date="${BASH_REMATCH[1]}"
  [[ "$(date -d "$part_date" +%F 2>/dev/null)" == "$part_date" ]] || die "Invalid date: $part_date"
  [[ -s "$path" ]] || die "Empty file: $path"
  [[ "$(head -n1 "$path")" == 'event_id,user_id,event_type,country,amount,event_ts' ]] \
    || die "Unexpected header in $filename"
  awk -F ',' 'NF != 6 { print "Bad CSV row " NR > "/dev/stderr"; bad=1 } END { exit bad }' "$path"
done

echo '==> Uploading input files to HDFS (the input directory is replaced, so reruns do not duplicate rows)'
"$HADOOP_HOME/bin/hdfs" dfs -rm -r -f -skipTrash "$HDFS_BASE/input"
"$HADOOP_HOME/bin/hdfs" dfs -mkdir -p "$HDFS_BASE/input"
"$HADOOP_HOME/bin/hdfs" dfs -put -f "${files[@]}" "$HDFS_BASE/input/"
"$HADOOP_HOME/bin/hdfs" dfs -ls "$HDFS_BASE/input"

run_job() {
  "$SPARK_HOME/bin/spark-submit" --master yarn --deploy-mode client --name hw4-events-etl \
    "$ROOT_DIR/jobs/events_etl.py" \
    --input "$NN$HDFS_BASE/input" --output-base "$NN$HDFS_BASE/warehouse" \
    --database hw4 --sink "$1"
}

echo '==> Running Spark on YARN: read, transform, save as partitioned tables (Hive Metastore)'
if run_job hive; then
  echo '==> Tables saved through the Hive Metastore'
else
  echo '==> Metastore write failed; fallback: partitioned Parquet in HDFS + external tables via beeline' >&2
  run_job path
  "$BEELINE" -u "$JDBC" -n team --silent=true -f "$ROOT_DIR/create_tables.sql"
fi

echo '==> Partitions after loading'
"$BEELINE" -u "$JDBC" -n team --silent=true --outputformat=tsv2 -e 'SHOW PARTITIONS hw4.daily_stats;'
echo '==> Done. Run ./scripts/verify.sh for checks.'
