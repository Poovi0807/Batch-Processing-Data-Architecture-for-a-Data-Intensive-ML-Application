"""QUARTERLY Spark job: processed zone -> curated zone + PostgreSQL.

    spark-submit build_features.py 2005Q3

reads   /data/processed/bgl/month=2005-07/, month=2005-08/, month=2005-09/
writes  /data/curated/features/quarter=2005Q3/      (Parquet; replaced on every run)
loads   table features.midplane_hourly in PostgreSQL (Airflow deletes the old rows first)
"""
import json
import os
import sys

from pyspark.sql import SparkSession
from pyspark.errors import AnalysisException

from transform import hourly_features

LAKE = os.environ.get("LAKE", "hdfs://hdfs-namenode:8020/data")
DB_URL = os.environ.get("DB_URL", "jdbc:postgresql://serving-db:5432/serving")
DB_USER = {"user": "etl_writer", "password": os.environ.get("ETL_PASSWORD", ""), "driver": "org.postgresql.Driver"}

quarter = sys.argv[1]                                   # e.g. 2005Q3
year, q = int(quarter[:4]), int(quarter[-1])
months = [f"{year}-{m:02d}" for m in range(3 * q - 2, 3 * q + 1)]

spark = SparkSession.builder.appName(f"build_features_{quarter}").getOrCreate()

# read the cleaned months of this quarter (the dataset does not have all of them)
tables = []
for month in months:
    try:
        tables.append(spark.read.parquet(f"{LAKE}/processed/bgl/month={month}"))
    except AnalysisException:
        print(f"no cleaned data for {month}")
if not tables:
    sys.exit(f"no cleaned data for {quarter}")
events = tables[0]
for t in tables[1:]:
    events = events.unionByName(t)

features = hourly_features(events, quarter).cache()
features.write.mode("overwrite").parquet(f"{LAKE}/curated/features/quarter={quarter}")
features.write.jdbc(DB_URL, "features.midplane_hourly", mode="append", properties=DB_USER)

report = {"events": events.count(), "feature_rows": features.count(),
          "positive_labels": features.filter("alert_next_hour").count()}
print("RESULT " + json.dumps(report))             # the last line is read by Airflow
spark.stop()
