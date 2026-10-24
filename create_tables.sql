-- Fallback only: used by scripts/load.sh when Spark cannot write through the Hive Metastore.
-- Spark has then written partitioned Parquet to HDFS, and the tables are registered here.
CREATE DATABASE IF NOT EXISTS hw4;

DROP TABLE IF EXISTS hw4.events_clean;
DROP TABLE IF EXISTS hw4.daily_stats;

CREATE EXTERNAL TABLE hw4.events_clean (
  event_id BIGINT,
  user_id INT,
  event_type STRING,
  country STRING,
  amount DECIMAL(12,2),
  event_hour INT
)
PARTITIONED BY (event_date DATE)
STORED AS PARQUET
LOCATION 'hdfs://10.22.0.11:9000/user/team/spark-hw4/warehouse/hw4.db/events_clean';

CREATE EXTERNAL TABLE hw4.daily_stats (
  country STRING,
  event_type STRING,
  events_cnt BIGINT,
  users_cnt BIGINT,
  total_amount DECIMAL(22,2),
  avg_amount DOUBLE,
  rank_in_day INT
)
PARTITIONED BY (event_date DATE)
STORED AS PARQUET
LOCATION 'hdfs://10.22.0.11:9000/user/team/spark-hw4/warehouse/hw4.db/daily_stats';

MSCK REPAIR TABLE hw4.events_clean;
MSCK REPAIR TABLE hw4.daily_stats;
