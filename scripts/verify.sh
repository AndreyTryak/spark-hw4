#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export HADOOP_HOME=/opt/hadoop-3.4.1
export HADOOP_CONF_DIR="$HADOOP_HOME/etc/hadoop"
export HIVE_HOME=/opt/apache-hive-4.0.0-alpha-2-bin
BEELINE="$HIVE_HOME/bin/beeline"
JDBC='jdbc:hive2://127.0.0.1:10000/default'

die() { printf 'ERROR: %s\n' "$*" >&2; exit 1; }

[[ -x "$BEELINE" ]] || die 'Missing beeline (run HW3 first).'
systemctl is-active --quiet hive-metastore || die 'Metastore not active.'
systemctl is-active --quiet hive-server2 || die 'HiveServer2 not active.'
"$HADOOP_HOME/bin/hdfs" dfs -test -d /user/team/spark-hw4/warehouse/hw4.db/events_clean \
  || die 'HDFS table directory missing (run ./scripts/load.sh).'

tmpdir="$(mktemp -d /tmp/spark-hw4-verify.XXXXXXXX)"
trap 'rm -rf -- "$tmpdir"' EXIT
hive() { "$BEELINE" -u "$JDBC" -n team --silent=true --outputformat=tsv2 --showHeader=false -e "$1"; }

echo '==> Spark application on YARN'
"$HADOOP_HOME/bin/yarn" application -list -appStates FINISHED 2>/dev/null \
  | grep hw4-events-etl | grep SUCCEEDED | tail -n1 \
  || die 'No successful hw4-events-etl application found in YARN.'

echo '==> Tables and partitions read with the standard Hive client (beeline)'
hive 'SHOW TABLES IN hw4;'
hive 'SHOW PARTITIONS hw4.daily_stats;' > "$tmpdir/partitions.txt"
cat "$tmpdir/partitions.txt"
hive 'SELECT * FROM hw4.events_clean LIMIT 5;'

echo '==> Expected result computed from the input files (independently of Spark)'
awk -F ',' -v out="$tmpdir/expected_stats_rows.txt" '
  FNR == 1 && $1 == "event_id" { next }
  NF == 6 && $1 ~ /^[0-9]+$/ && $2 ~ /^[0-9]+$/ && $3 != "" \
    && $5 ~ /^-?[0-9]+(\.[0-9]+)?$/ && $6 ~ /^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9:]{8}$/ && !seen[$1]++ {
      d = substr($6, 1, 10); c[d]++; s[d] += $5; k[d SUBSEP $4 SUBSEP $3] = 1
  }
  END {
    for (d in c) printf "%s\t%d\t%.2f\n", d, c[d], s[d]
    n = 0; for (x in k) n++
    print n > out
  }' "$ROOT_DIR"/data/events_*.csv | LC_ALL=C sort > "$tmpdir/expected.sorted"
cat "$tmpdir/expected.sorted"
expected=$(awk -F '\t' '{ t += $2 } END { print t }' "$tmpdir/expected.sorted")
expected_stats=$(cat "$tmpdir/expected_stats_rows.txt")

echo '==> Hive: rows and amounts per partition of hw4.events_clean'
hive 'SELECT event_date, COUNT(*), SUM(amount) FROM hw4.events_clean GROUP BY event_date;' \
  | LC_ALL=C sort > "$tmpdir/actual.sorted"
cat "$tmpdir/actual.sorted"
diff -u "$tmpdir/expected.sorted" "$tmpdir/actual.sorted"
echo 'PASS: events_clean in Hive matches the input files.'

while IFS=$'\t' read -r part_date _; do
  grep -Fxq "event_date=$part_date" "$tmpdir/partitions.txt" || die "Missing partition $part_date in daily_stats."
done < "$tmpdir/expected.sorted"
actual_stats=$(hive 'SELECT COUNT(*) FROM hw4.daily_stats;' | tr -d '[:space:]')
[[ "$actual_stats" == "$expected_stats" ]] \
  || die "daily_stats has $actual_stats rows, expected $expected_stats."
echo "PASS: daily_stats has $actual_stats rows, one partition per day."

echo '==> HDFS warehouse directory'
"$HADOOP_HOME/bin/hdfs" dfs -ls /user/team/spark-hw4/warehouse/hw4.db/events_clean
"$HADOOP_HOME/bin/hdfs" dfsadmin -report | grep 'Live datanodes (3)'
"$HADOOP_HOME/bin/yarn" node -list -states RUNNING | grep -E 'Total Nodes:[[:space:]]*3'
echo "PASS: Spark on YARN, $expected rows in Hive match input files, daily_stats=$actual_stats rows, HDFS=3, YARN=3."
