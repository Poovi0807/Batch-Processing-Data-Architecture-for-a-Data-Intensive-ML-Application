"""Helpers shared by the pipeline DAGs: settings and calls to the other microservices."""
import os
import time

import pendulum
import requests

INGEST_URL = os.environ.get("INGEST_URL", "http://ingest:8000")
SPARK_GATEWAY_URL = os.environ.get("SPARK_GATEWAY_URL", "http://spark-gateway:8000")
WEBHDFS_URL = os.environ.get("WEBHDFS_URL", "http://hdfs-namenode:9870/webhdfs/v1")
HDFS_USER = os.environ.get("HDFS_USER", "etl")

# months covered by the dataset; the DAGs only schedule runs inside this range
FIRST_MONTH = os.environ.get("PIPELINE_FIRST_MONTH", "2005-06")
LAST_MONTH = os.environ.get("PIPELINE_LAST_MONTH", "2006-01")


def month_start(ym: str) -> pendulum.DateTime:
    year, month = map(int, ym.split("-"))
    return pendulum.datetime(year, month, 1, tz="UTC")


def quarter_start(dt: pendulum.DateTime) -> pendulum.DateTime:
    return pendulum.datetime(dt.year, 3 * ((dt.month - 1) // 3) + 1, 1, tz="UTC")


def months_in_range(year: int, months: list[int]) -> list[int]:
    lo, hi = month_start(FIRST_MONTH), month_start(LAST_MONTH)
    return [m for m in months if lo <= pendulum.datetime(year, m, 1, tz="UTC") <= hi]


def target_month(context) -> tuple[int, int]:
    """Month to process: the "month" parameter (YYYY-MM) of a manual run, otherwise the
    start of the run's data interval (scheduled and catch-up runs)."""
    chosen = (context["params"] or {}).get("month") or ""
    if chosen:
        year, month = map(int, chosen.split("-"))
        return year, month
    start = context["data_interval_start"]
    return start.year, start.month


def target_quarter(context) -> tuple[int, int]:
    """Quarter to process: the "quarter" parameter (YYYYQn) of a manual run, otherwise the
    quarter in which the run's data interval starts."""
    chosen = (context["params"] or {}).get("quarter") or ""
    if chosen:
        year, quarter = chosen.upper().split("Q")
        return int(year), int(quarter)
    start = context["data_interval_start"]
    return start.year, (start.month - 1) // 3 + 1


def in_dataset(year: int, month: int) -> bool:
    return month_start(FIRST_MONTH) <= pendulum.datetime(year, month, 1, tz="UTC") <= month_start(LAST_MONTH)


def call_ingest(year: int, month: int) -> dict:
    r = requests.post(f"{INGEST_URL}/ingest", json={"year": year, "month": month}, timeout=3600)
    body = r.json()
    if r.status_code != 200:
        raise RuntimeError(f"ingestion failed ({r.status_code}): {body}")
    return body


def run_spark_job(job: str, args: list[int], poll_seconds: int = 15, timeout_s: int = 7200) -> dict:
    """Start a job on the Spark gateway and wait until it has finished."""
    r = requests.post(f"{SPARK_GATEWAY_URL}/jobs", json={"job": job, "args": args}, timeout=30)
    r.raise_for_status()
    job_id = r.json()["job_id"]
    print(f"started Spark job {job_id}")
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        time.sleep(poll_seconds)
        status = requests.get(f"{SPARK_GATEWAY_URL}/jobs/{job_id}", timeout=30).json()
        if status["status"] != "running":
            print(status.get("log_tail", ""))
            if status["status"] != "succeeded":
                raise RuntimeError(f"Spark job {job_id} failed (return code {status['returncode']})")
            return status
    raise TimeoutError(f"Spark job {job_id} did not finish within {timeout_s} s")


def hdfs_path_exists(path: str) -> bool:
    r = requests.get(f"{WEBHDFS_URL}{path}", params={"op": "GETFILESTATUS", "user.name": HDFS_USER},
                     timeout=30)
    if r.status_code == 404:
        return False
    r.raise_for_status()
    return True
