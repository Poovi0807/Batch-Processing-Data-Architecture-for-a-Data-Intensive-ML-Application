"""The work behind each Airflow task. Each function is one step of the pipeline.

Settings come from environment variables that docker-compose.yml sets:
  SPARK_REMOTE    sc://spark-connect:15002       (Spark Connect server)
  LAKE_BASE       hdfs://namenode:8020/bgl       (data lake root)
  INGEST_URL      http://ingest:8000             (ingestion service)
  SERVING_DB_DSN  connection to the serving database
"""
import os
import socket
from urllib.parse import urlparse

import requests
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from jobs import db
from jobs.periods import check_month, check_quarter, months_of_quarter
from jobs.transform import clean, hourly_features, parse_lines

MAX_DROP_RATE = 0.01   # quality gate: at most 1 % of the lines may be dropped


def lake(path: str) -> str:
    return f"{os.environ['LAKE_BASE'].rstrip('/')}/{path}"


def spark_session(app_name: str) -> SparkSession:
    """Open a Spark Connect session, or fail fast with a clear message.

    Without the check, the Spark client silently retries for several minutes when
    the server is down. Failing fast lets Airflow's own retries take over.
    """
    remote = urlparse(os.environ["SPARK_REMOTE"].replace("sc://", "http://", 1))
    try:
        socket.create_connection((remote.hostname, remote.port or 15002), timeout=10).close()
    except OSError as exc:
        raise ConnectionError(f"Spark Connect server {remote.hostname}:{remote.port} "
                              f"is not reachable: {exc}") from exc
    return SparkSession.builder.remote(os.environ["SPARK_REMOTE"]).appName(app_name).getOrCreate()


# ---------- monthly steps ----------

def ingest_month(month: str) -> dict:
    """Ask the ingestion service to copy the month's log file into the raw zone."""
    check_month(month)
    with db.audited("ingest", month) as record:
        response = requests.post(f"{os.environ['INGEST_URL']}/ingest",
                                 json={"month": month}, timeout=1800)
        if response.status_code >= 400:
            raise RuntimeError(f"Ingestion service answered {response.status_code}: {response.text}")
        result = response.json()
        record["rows_in"] = record["rows_out"] = result["line_count"]
        record["message"] = result["status"]
    return result


def clean_month(month: str) -> dict:
    """Raw text -> cleaned, masked Parquet in the processed zone."""
    check_month(month)
    with db.audited("clean", month) as record:
        spark = spark_session(f"clean {month}")
        try:
            raw = spark.read.text(lake(f"raw/month={month}"))
            record["rows_in"] = raw.count()
            events = clean(parse_lines(raw), month)
            events.write.mode("overwrite").parquet(lake(f"processed/month={month}"))
            record["rows_out"] = spark.read.parquet(lake(f"processed/month={month}")).count()
        finally:
            spark.stop()
    return {"month": month, "rows_in": record["rows_in"], "rows_out": record["rows_out"]}


def check_month_quality(stats: dict) -> dict:
    """Stop the pipeline if a month looks wrong instead of passing bad data on."""
    month = stats["month"]
    with db.audited("quality_check", month) as record:
        record["rows_in"], record["rows_out"] = stats["rows_in"], stats["rows_out"]
        if stats["rows_out"] == 0:
            raise ValueError(f"{month}: no valid lines after cleaning")
        drop_rate = 1 - stats["rows_out"] / stats["rows_in"]
        if drop_rate > MAX_DROP_RATE:
            raise ValueError(f"{month}: {drop_rate:.2%} of lines dropped (limit {MAX_DROP_RATE:.0%})")
        record["message"] = f"drop rate {drop_rate:.3%}"
    return stats


# ---------- quarterly steps ----------

def aggregate_quarter(quarter: str) -> dict:
    """Processed events of the quarter -> hourly features per midplane (curated zone)."""
    check_quarter(quarter)
    with db.audited("aggregate", quarter) as record:
        spark = spark_session(f"aggregate {quarter}")
        try:
            # Keep '2005-06' as text instead of guessing a date type from the folder name.
            spark.conf.set("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
            events = (spark.read.parquet(lake("processed"))
                      .filter(F.col("month").isin(months_of_quarter(quarter))))
            record["rows_in"] = events.count()
            if record["rows_in"] == 0:
                raise ValueError(f"No processed data for {quarter}. Run the monthly DAG first.")
            features = hourly_features(events, quarter)
            features.write.mode("overwrite").parquet(lake(f"curated/quarter={quarter}"))
            record["rows_out"] = spark.read.parquet(lake(f"curated/quarter={quarter}")).count()
        finally:
            spark.stop()
    return {"quarter": quarter, "events": record["rows_in"], "feature_rows": record["rows_out"]}


def deliver_quarter(quarter: str) -> dict:
    """Load the curated features into PostgreSQL, where the ML application reads them."""
    check_quarter(quarter)
    with db.audited("deliver", quarter) as record:
        spark = spark_session(f"deliver {quarter}")
        try:
            features = spark.read.parquet(lake(f"curated/quarter={quarter}"))
            rows = [tuple(r[c] for c in db.FEATURE_COLUMNS[1:]) for r in features.collect()]
        finally:
            spark.stop()
        rows = [(quarter, *row) for row in rows]
        record["rows_in"] = len(rows)
        record["rows_out"] = db.replace_quarter(quarter, rows)
        if record["rows_out"] != record["rows_in"]:
            raise ValueError("Row count in the database does not match the curated zone")
    return {"quarter": quarter, "rows_loaded": record["rows_out"]}
