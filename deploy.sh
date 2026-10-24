#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
HADOOP_HOME=/opt/hadoop-3.4.1
SPARK_VERSION=3.5.3
SPARK_HOME="/opt/spark-${SPARK_VERSION}-bin-hadoop3"
JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
HADOOP_CONF_DIR="$HADOOP_HOME/etc/hadoop"
export HADOOP_HOME HADOOP_CONF_DIR JAVA_HOME
HDFS_BASE=/user/team/spark-hw4

die() { printf 'ERROR: %s\n' "$*" >&2; exit 1; }
log() { printf '\n==> %s\n' "$*"; }

[[ "$(id -un)" == team ]] || die 'Run as user team on the edge VM.'
[[ "$(hostname -s)" == team-22-en ]] || die 'Run on team-22-en.'
[[ -x "$HADOOP_HOME/bin/hdfs" ]] || die "Missing HDFS client at $HADOOP_HOME (run HW1 first)."
[[ -x "$HADOOP_HOME/bin/yarn" ]] || die 'Missing YARN client (run HW2 first).'
[[ -x "$JAVA_HOME/bin/java" ]] || die 'Missing Java 11 (run HW1 first).'
command -v sudo >/dev/null || die 'sudo is required.'
sudo -n true
if [[ -e "$SPARK_HOME" && ! -f "$SPARK_HOME/.spark-hw4-owned" ]]; then
  # installation made by an earlier run of this deploy.sh, before the marker file existed
  grep -qE 'spark-hw4|HW4' "$SPARK_HOME/conf/spark-defaults.conf" 2>/dev/null \
    && touch "$SPARK_HOME/.spark-hw4-owned" \
    || die 'Existing Spark installation is not owned by HW4.'
fi

log 'Checking HDFS, YARN and Hive baseline'
"$HADOOP_HOME/bin/hdfs" dfsadmin -report | grep -Eq 'Live datanodes \(3\)' \
  || die 'Expected exactly 3 live DataNodes.'
"$HADOOP_HOME/bin/yarn" node -list | grep -Eq 'Total Nodes:[[:space:]]*3' \
  || die 'Expected exactly 3 NodeManagers.'
for unit in hive-metastore hive-server2; do
  systemctl is-active --quiet "$unit" || die "$unit is not active (run HW3 first)."
done

log 'Checking python3 on all VMs (PySpark executors need it)'
SSH=(ssh -o IdentitiesOnly=yes -o StrictHostKeyChecking=accept-new -i "$HOME/.ssh/team_internal")
command -v python3 >/dev/null || sudo apt-get install -y python3
for host in 10.22.0.11 10.22.0.12 10.22.0.13; do
  "${SSH[@]}" "team@$host" 'command -v python3 >/dev/null || sudo apt-get install -y python3'
  echo "python3 OK on $host"
done

log 'Installing and verifying official Apache Spark archive'
if [[ ! -x "$SPARK_HOME/bin/spark-submit" ]]; then
  download_dir="$(mktemp -d /tmp/spark-hw4-download.XXXXXXXX)"
  trap 'rm -rf -- "$download_dir"' EXIT
  filename="spark-${SPARK_VERSION}-bin-hadoop3.tgz"
  base_url="https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}"
  curl --fail --location --retry 3 --output "$download_dir/$filename" "$base_url/$filename"
  curl --fail --location --retry 3 --output "$download_dir/$filename.sha512" "$base_url/$filename.sha512"
  actual="$(sha512sum "$download_dir/$filename" | cut -d' ' -f1)"
  expected="$(tr -d ' \n' < "$download_dir/$filename.sha512" | tr 'A-F' 'a-f')"
  [[ "$expected" == *"$actual"* ]] || die 'Spark archive checksum mismatch!'
  sudo tar -xzf "$download_dir/$filename" -C /opt
  sudo chown -R team:team "$SPARK_HOME"
  touch "$SPARK_HOME/.spark-hw4-owned"
  rm -rf -- "$download_dir"
  trap - EXIT
fi
[[ -x "$SPARK_HOME/bin/spark-submit" ]] || die 'spark-submit missing.'
sudo ln -sfn "$SPARK_HOME" /opt/spark

log 'Configuring Spark for YARN and the HW3 Metastore'
install -m 644 "$ROOT_DIR/config/spark-defaults.conf" "$SPARK_HOME/conf/spark-defaults.conf"
install -m 644 "$ROOT_DIR/config/hive-site.xml" "$SPARK_HOME/conf/hive-site.xml"
install -m 755 "$ROOT_DIR/config/spark-env.sh" "$SPARK_HOME/conf/spark-env.sh"

log 'Preparing HDFS directories and the Spark jars archive for YARN'
"$HADOOP_HOME/bin/hdfs" dfs -mkdir -p "$HDFS_BASE/input" "$HDFS_BASE/events" "$HDFS_BASE/warehouse"
if ! "$HADOOP_HOME/bin/hdfs" dfs -test -e "$HDFS_BASE/spark-libs.jar"; then
  tmp="$(mktemp -d /tmp/spark-hw4-libs.XXXXXXXX)"
  "$JAVA_HOME/bin/jar" c0f "$tmp/spark-libs.jar" -C "$SPARK_HOME/jars/" .
  "$HADOOP_HOME/bin/hdfs" dfs -put -f "$tmp/spark-libs.jar" "$HDFS_BASE/spark-libs.jar"
  rm -rf -- "$tmp"
fi

log 'Smoke test: SparkPi on YARN'
"$SPARK_HOME/bin/spark-submit" --master yarn --deploy-mode client \
  "$SPARK_HOME/examples/src/main/python/pi.py" 10 2>&1 | grep -E 'Pi is roughly|ERROR' \
  || die 'SparkPi on YARN failed (run it without grep to see the log).'
"$SPARK_HOME/bin/spark-submit" --version 2>&1 | grep -E 'version|Scala'
log 'Deployment complete. Next: ./scripts/load.sh && ./scripts/verify.sh'
