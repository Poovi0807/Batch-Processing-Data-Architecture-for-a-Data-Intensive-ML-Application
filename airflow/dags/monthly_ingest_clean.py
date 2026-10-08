"""Monthly DAG: ingest the month's log file into HDFS, then parse and clean it with Spark.

Each run processes the month of its data interval (e.g. the run for 2005-06-01 .. 2005-07-01
processes June 2005). The historical months of the dataset are replayed with an Airflow
backfill (see README); afterwards the DAG would keep running on the 1st of every month.
Months outside the dataset (PIPELINE_FIRST_MONTH..PIPELINE_LAST_MONTH) are skipped, because
no log file is delivered for them. A single month can be (re)processed by triggering the
DAG manually with the parameter month = "YYYY-MM".
"""
from datetime import timedelta

from airflow.sdk.exceptions import AirflowSkipException
from airflow.sdk import Param, dag, get_current_context, task
from airflow.timetables.interval import CronDataIntervalTimetable

from pipeline_common import FIRST_MONTH, call_ingest, in_dataset, month_start, run_spark_job, target_month

default_args = {
    "owner": "data-engineering",
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
}


@dag(
    dag_id="monthly_ingest_clean",
    description="Ingest the monthly BGL log file and clean it (raw -> processed zone)",
    schedule=CronDataIntervalTimetable("0 0 1 * *", timezone="UTC"),  # 1st of every month, 00:00 UTC
    start_date=month_start(FIRST_MONTH),
    catchup=False,  # history is replayed with "airflow backfill create"
    max_active_runs=1,
    default_args=default_args,
    tags=["bgl", "monthly", "batch"],
    params={"month": Param("", type="string", pattern=r"^(\d{4}-\d{2})?$",
                           description="Manual runs only: month to process, e.g. 2005-06")},
)
def monthly_ingest_clean():

    @task(execution_timeout=timedelta(hours=1))
    def ingest_raw() -> dict:
        year, month = target_month(get_current_context())
        if not in_dataset(year, month):
            raise AirflowSkipException(f"no log file is delivered for {year}-{month:02d} (outside the dataset)")
        result = call_ingest(year, month)
        print(f"ingested {result['lines']:,} lines into {result['hdfs_path']} "
              f"(sha256 {result['sha256'][:12]}..., malformed {result['malformed_lines']})")
        return {"year": year, "month": month}

    @task(execution_timeout=timedelta(hours=2))
    def parse_clean(partition: dict) -> None:
        run_spark_job("parse_clean_month", [partition["year"], partition["month"]])

    parse_clean(ingest_raw())


monthly_ingest_clean()
