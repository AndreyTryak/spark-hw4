#!/usr/bin/env python3
import argparse

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

p = argparse.ArgumentParser()
p.add_argument("--input", default="hdfs://10.22.0.11:9000/user/team/spark-hw4/input")
p.add_argument("--output-base", default="hdfs://10.22.0.11:9000/user/team/spark-hw4/warehouse")
p.add_argument("--database", default="hw4")
p.add_argument("--sink", choices=["hive", "path"], default="hive")
args = p.parse_args()

builder = SparkSession.builder.appName("hw4-events-etl")
if args.sink == "hive":
    builder = builder.enableHiveSupport()
spark = builder.getOrCreate()
spark.sparkContext.setLogLevel("WARN")

print("=== Spark session ===")
print("master       :", spark.sparkContext.master)
print("applicationId:", spark.sparkContext.applicationId)
print("version      :", spark.version)

raw = (spark.read
       .option("header", True)
       .option("inferSchema", False)
       .csv(args.input))
raw_cnt = raw.count()
print(f"raw rows: {raw_cnt}")
raw.printSchema()

typed = (raw
         .withColumn("event_id", F.col("event_id").cast("long"))
         .withColumn("user_id", F.col("user_id").cast("int"))
         .withColumn("amount", F.col("amount").cast("decimal(12,2)"))
         .withColumn("event_ts", F.to_timestamp("event_ts", "yyyy-MM-dd HH:mm:ss"))
         .withColumn("event_type", F.lower(F.trim("event_type")))
         .withColumn("country", F.upper(F.trim("country"))))

clean = (typed
         .filter(F.col("event_id").isNotNull() & F.col("user_id").isNotNull()
                 & F.col("event_ts").isNotNull() & F.col("amount").isNotNull()
                 & F.col("event_type").isNotNull() & (F.col("event_type") != ""))
         .dropDuplicates(["event_id"]))

events_clean = (clean
                .withColumn("event_hour", F.hour("event_ts"))
                .withColumn("event_date", F.to_date("event_ts"))
                .select("event_id", "user_id", "event_type", "country",
                        "amount", "event_hour", "event_date"))

agg = (events_clean
       .groupBy("event_date", "country", "event_type")
       .agg(F.count("*").alias("events_cnt"),
            F.countDistinct("user_id").alias("users_cnt"),
            F.sum("amount").alias("total_amount"),
            F.round(F.avg("amount").cast("double"), 2).alias("avg_amount")))

w = Window.partitionBy("event_date").orderBy(F.col("total_amount").desc())
daily_stats = (agg
               .withColumn("rank_in_day", F.dense_rank().over(w))
               .select("country", "event_type", "events_cnt", "users_cnt",
                       "total_amount", "avg_amount", "rank_in_day", "event_date"))

clean_cnt = events_clean.count()
print(f"clean rows: {clean_cnt} (dropped {raw_cnt - clean_cnt})")

events_out = events_clean.repartition("event_date")
stats_out = daily_stats.repartition("event_date")

if args.sink == "hive":
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {args.database} "
              f"LOCATION '{args.output_base}/{args.database}.db'")
    for name, df in (("events_clean", events_out), ("daily_stats", stats_out)):
        (df.write
           .format("hive")
           .option("fileFormat", "parquet")
           .mode("overwrite")
           .partitionBy("event_date")
           .saveAsTable(f"{args.database}.{name}"))
        print(f"saved table {args.database}.{name}")
    print("partitions of daily_stats:")
    spark.sql(f"SHOW PARTITIONS {args.database}.daily_stats").show(truncate=False)
    n_clean = spark.table(f"{args.database}.events_clean").count()
    n_stats = spark.table(f"{args.database}.daily_stats").count()
else:
    for name, df in (("events_clean", events_out), ("daily_stats", stats_out)):
        (df.write.mode("overwrite").partitionBy("event_date")
           .parquet(f"{args.output_base}/{args.database}.db/{name}"))
        print(f"saved parquet {args.output_base}/{args.database}.db/{name}")
    n_clean = spark.read.parquet(f"{args.output_base}/{args.database}.db/events_clean").count()
    n_stats = spark.read.parquet(f"{args.output_base}/{args.database}.db/daily_stats").count()

daily_stats.orderBy("event_date", "rank_in_day").show(10, truncate=False)

print(f"RESULT events_clean={n_clean} daily_stats={n_stats} raw={raw_cnt}")
spark.stop()
