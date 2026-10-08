"""Quarterly DAG: wait for the quarter's cleaned months, aggregate ML features with Spark,
load them into the serving database and check that the ML application can read them.
"""
import os
from datetime import timedelta

from airflow.sdk.exceptions import AirflowSkipException
from airflow.sdk import Param, dag, get_current_context, task
from airflow.timetables.interval import CronDataIntervalTimetable

from pipeline_common import (FIRST_MONTH, hdfs_path_exists, month_start, months_in_range,
                             quarter_start, run_spark_job, target_quarter)

default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def quarter_of_run() -> tuple[int, int]:
    return target_quarter(get_current_context())


@dag(
    dag_id="quarterly_aggregate_deliver",
    description="Aggregate hourly features per midplane and deliver them to PostgreSQL",
    schedule=CronDataIntervalTimetable("0 0 1 1,4,7,10 *", timezone="UTC"),
    start_date=quarter_start(month_start(FIRST_MONTH)),
    catchup=False,  # history is replayed with "airflow backfill create"
    max_active_runs=1,
    default_args=default_args,
    tags=["bgl", "quarterly", "batch"],
    params={"quarter": Param("", type="string", pattern=r"^(\d{4}[Qq][1-4])?$",
                             description="Manual runs only: quarter to process, e.g. 2005Q3")},
)
def quarterly_aggregate_deliver():

    # waits (without blocking a worker slot) until every month of the quarter is cleaned
    @task.sensor(poke_interval=60, timeout=24 * 3600, mode="reschedule")
    def wait_for_cleaned_months() -> bool:
        year, quarter = quarter_of_run()
        months = months_in_range(year, [3 * (quarter - 1) + i for i in (1, 2, 3)])
        if not months:
            raise AirflowSkipException(f"{year}Q{quarter} lies outside the dataset")
        missing = [m for m in months
                   if not hdfs_path_exists(f"/data/processed/bgl/year={year:04d}/month={m:02d}/_SUCCESS")]
        print(f"{year}Q{quarter}: months {months}, still missing {missing}")
        return not missing

    @task(execution_timeout=timedelta(hours=2))
    def aggregate_and_deliver() -> str:
        year, quarter = quarter_of_run()
        run_spark_job("aggregate_quarter", [year, quarter])
        return f"{year:04d}Q{quarter}"

    @task
    def verify_delivery(label: str) -> None:
        """Connect as the read-only ML user, exactly like the ML application would."""
        import psycopg2  # imported here to keep DAG parsing fast

        conn = psycopg2.connect(
            host=os.environ["SERVING_DB_HOST"], dbname=os.environ["SERVING_DB_NAME"],
            user=os.environ["SERVING_ML_USER"], password=os.environ["SERVING_ML_PASSWORD"])
        with conn, conn.cursor() as cur:
            cur.execute("SELECT count(*), count(*) FILTER (WHERE alert_next_hour), "
                        "count(DISTINCT midplane) FROM features.midplane_hourly WHERE quarter = %s",
                        (label,))
            rows, positives, midplanes = cur.fetchone()
        conn.close()
        print(f"{label}: {rows:,} feature rows, {midplanes} midplanes, {positives:,} positive labels")
        if rows == 0:
            raise ValueError(f"no feature rows delivered for {label}")

    label = aggregate_and_deliver()
    wait_for_cleaned_months() >> label
    verify_delivery(label)


quarterly_aggregate_deliver()
