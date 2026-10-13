"""MONTHLY Spark job: raw zone -> processed zone.

    spark-submit clean_month.py 2005-06

reads   /data/raw/bgl/month=2005-06/          (the log file, unchanged)
writes  /data/processed/bgl/month=2005-06/    (clean table, Parquet; replaced on every run)
"""
import json
import os
import sys

from pyspark.sql import SparkSession

from transform import clean_lines

LAKE = os.environ.get("LAKE", "hdfs://hdfs-namenode:8020/data")
MAX_BROKEN = 0.01

month = sys.argv[1]
spark = SparkSession.builder.appName(f"clean_month_{month}").getOrCreate()

raw = spark.read.text(f"{LAKE}/raw/bgl/month={month}")
clean, report = clean_lines(raw)

if report["broken_ratio"] > MAX_BROKEN:          # data-quality gate
    print("RESULT " + json.dumps(report))
    sys.exit(f"too many broken lines in {month}")

clean.write.mode("overwrite").parquet(f"{LAKE}/processed/bgl/month={month}")
print("RESULT " + json.dumps(report))             # the last line is read by Airflow
spark.stop()
