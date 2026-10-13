"""The two Airflow pipelines (DAGs) of the project.

monthly_ingest_clean        once per month    ingest  ->  clean
quarterly_features_deliver  once per quarter  wait for the 3 months  ->  delete old rows
                                              ->  build features  ->  check delivery

Both DAGs cover the dataset (June 2005 - January 2006). Because catchup=True, Airflow
runs all past months and quarters automatically as soon as it starts.
Every step writes one line into the audit table governance.pipeline_runs.
"""
import json
import os
from datetime import timedelta
from pathlib import Path

import pendulum
import psycopg2
import requests
from airflow.sdk import dag, task
from airflow.timetables.interval import CronDataIntervalTimetable

INGEST = os.environ.get("INGEST_URL", "http://ingest:8000")
SPARK = os.environ.get("SPARK_GATEWAY_URL", "http://spark-gateway:8000")
WEBHDFS = os.environ.get("WEBHDFS_URL", "http://hdfs-namenode:9870/webhdfs/v1")
LANDING = os.environ.get("LANDING_DIR", "/data/landing")
DB_HOST = os.environ.get("SERVING_DB_HOST", "serving-db")
DB_PORT = int(os.environ.get("SERVING_DB_PORT", "5432"))

RETRIES = {"retries": 2, "retry_delay": timedelta(minutes=2)}


# ------------------------------------------------------------------ helpers
def db(user: str):
    """Connect to the serving database as etl_writer or ml_reader."""
    password = os.environ["ETL_PASSWORD"] if user == "etl_writer" else os.environ["ML_PASSWORD"]
    return psycopg2.connect(host=DB_HOST, port=DB_PORT, dbname="serving", user=user, password=password)


def audit(stage: str, key: str, status: str, rows_in=None, rows_out=None, details=None) -> None:
    """Write one line into the audit table."""
    with db("etl_writer") as conn, conn.cursor() as cur:
        cur.execute("INSERT INTO governance.pipeline_runs (stage, partition_key, status, rows_in, rows_out, details) "
                    "VALUES (%s, %s, %s, %s, %s, %s)", (stage, key, status, rows_in, rows_out, json.dumps(details or {})))


def call(stage: str, key: str, url: str) -> dict:
    """Call another service and wait for its answer; record failures in the audit table."""
    answer = requests.post(url, timeout=3 * 3600)
    if answer.status_code != 200:
        audit(stage, key, "failed", details={"answer": answer.text[-2000:]})
        raise RuntimeError(f"{stage} {key} failed: {answer.text[-2000:]}")
    return answer.json()


# ------------------------------------------------------------------ monthly DAG
@dag(schedule=CronDataIntervalTimetable("0 0 1 * *", timezone="UTC"),       # 1st of every month
     start_date=pendulum.datetime(2005, 6, 1, tz="UTC"), end_date=pendulum.datetime(2006, 1, 31, tz="UTC"),
     catchup=True, max_active_runs=1, default_args=RETRIES, tags=["bgl"])
def monthly_ingest_clean():

    @task
    def ingest(data_interval_start=None) -> str:
        month = data_interval_start.strftime("%Y-%m")                       # e.g. 2005-06
        report = call("ingest", month, f"{INGEST}/ingest?month={month}")
        audit("ingest", month, "success", report["lines"], report["lines"] - report["broken_lines"], report)
        return month

    @task
    def clean(month: str) -> None:
        report = call("clean", month, f"{SPARK}/run?job=clean_month&arg={month}")
        audit("clean", month, "success", report["lines_in"], report["lines_out"], report)

    clean(ingest())


# ------------------------------------------------------------------ quarterly DAG
@dag(schedule=CronDataIntervalTimetable("0 0 1 1,4,7,10 *", timezone="UTC"),  # 1st of every quarter
     start_date=pendulum.datetime(2005, 4, 1, tz="UTC"), end_date=pendulum.datetime(2006, 1, 31, tz="UTC"),
     catchup=True, max_active_runs=1, default_args=RETRIES, tags=["bgl"])
def quarterly_features_deliver():

    @task
    def which_quarter(data_interval_start=None) -> str:
        return f"{data_interval_start.year}Q{(data_interval_start.month - 1) // 3 + 1}"   # e.g. 2005Q3

    @task.sensor(poke_interval=60, timeout=6 * 3600, mode="reschedule")
    def wait_for_months(quarter: str) -> bool:
        """Wait until every month of the quarter that has a log file has been cleaned."""
        year, q = int(quarter[:4]), int(quarter[-1])
        months = [f"{year}-{m:02d}" for m in range(3 * q - 2, 3 * q + 1)]
        months = [m for m in months if Path(f"{LANDING}/bgl_{m}.log").exists()]
        missing = [m for m in months if requests.get(
            f"{WEBHDFS}/data/processed/bgl/month={m}/_SUCCESS",
            params={"op": "GETFILESTATUS", "user.name": "etl"}, timeout=30).status_code != 200]
        print(f"{quarter}: months {months}, not cleaned yet: {missing}")
        return not missing

    @task
    def delete_old_rows(quarter: str) -> None:
        """Running a quarter again must replace it, not add it twice."""
        with db("etl_writer") as conn, conn.cursor() as cur:
            cur.execute("DELETE FROM features.midplane_hourly WHERE quarter = %s", (quarter,))

    @task
    def build_features(quarter: str) -> None:
        report = call("features", quarter, f"{SPARK}/run?job=build_features&arg={quarter}")
        audit("features", quarter, "success", report["events"], report["feature_rows"], report)

    @task
    def check_delivery(quarter: str) -> None:
        """Read the new rows exactly like the ML application does (read-only user)."""
        with db("ml_reader") as conn, conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM features.midplane_hourly WHERE quarter = %s", (quarter,))
            rows = cur.fetchone()[0]
        print(f"{quarter}: {rows:,} feature rows can be read by ml_reader")
        if rows == 0:
            raise ValueError(f"nothing delivered for {quarter}")

    quarter = which_quarter()
    wait_for_months(quarter) >> delete_old_rows(quarter) >> build_features(quarter) >> check_delivery(quarter)


monthly_ingest_clean()
quarterly_features_deliver()
