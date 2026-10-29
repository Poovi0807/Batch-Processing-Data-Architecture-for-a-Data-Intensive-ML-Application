"""Two DAGs for the BGL batch pipeline.

bgl_monthly_ingest_clean        runs once per month (June 2005 to January 2006):
    ingest -> clean -> quality_check -> [quarter complete?] -> trigger quarterly DAG

bgl_quarterly_aggregate_deliver runs when the monthly DAG triggers it, or by hand:
    aggregate -> deliver
"""
from datetime import datetime, timedelta

from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import Param, dag, task

from jobs import tasks
from jobs.periods import is_quarter_end, quarter_of

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "retries": 3,                          # try again after short failures
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(hours=2),
}

MONTH = "{{ data_interval_start.strftime('%Y-%m') }}"   # e.g. 2005-06
LAST_MONTH = "2006-01"                                    # last month in the dataset


@dag(
    dag_id="bgl_monthly_ingest_clean",
    description="Ingest one month of BGL logs and clean it",
    schedule="@monthly",
    start_date=datetime(2005, 6, 1),
    end_date=datetime(2006, 1, 31),       # the dataset ends in January 2006
    catchup=True,                          # replay every month of the dataset in order
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["bgl", "monthly"],
)
def monthly_ingest_clean():

    @task
    def ingest(month: str) -> dict:
        return tasks.ingest_month(month)

    @task
    def clean(month: str) -> dict:
        return tasks.clean_month(month)

    @task
    def quality_check(stats: dict) -> dict:
        return tasks.check_month_quality(stats)

    @task.short_circuit
    def quarter_complete(month: str):
        """Continue only after the last month of a quarter (or of the dataset)."""
        if is_quarter_end(month) or month == LAST_MONTH:
            return quarter_of(month)
        return False

    trigger_quarterly = TriggerDagRunOperator(
        task_id="trigger_quarterly",
        trigger_dag_id="bgl_quarterly_aggregate_deliver",
        conf={"quarter": "{{ ti.xcom_pull(task_ids='quarter_complete') }}"},
    )

    ingested = ingest(MONTH)
    cleaned = clean(MONTH)
    ingested >> cleaned                    # clean only after the file is in the raw zone
    quality_check(cleaned) >> quarter_complete(MONTH) >> trigger_quarterly


@dag(
    dag_id="bgl_quarterly_aggregate_deliver",
    description="Build hourly features for one quarter and deliver them to PostgreSQL",
    schedule=None,                         # started by the monthly DAG or by hand
    start_date=datetime(2005, 1, 1),
    catchup=False,
    is_paused_upon_creation=False,         # on from the start: only the monthly DAG needs switching on
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    params={"quarter": Param("2005Q3", type="string", pattern=r"^\d{4}Q[1-4]$",
                             description="Quarter to build, for example 2005Q3")},
    tags=["bgl", "quarterly"],
)
def quarterly_aggregate_deliver():

    @task
    def aggregate(quarter: str) -> dict:
        return tasks.aggregate_quarter(quarter)

    @task
    def deliver(quarter: str) -> dict:
        return tasks.deliver_quarter(quarter)

    quarter = "{{ params.quarter }}"
    aggregate(quarter) >> deliver(quarter)


monthly_ingest_clean()
quarterly_aggregate_deliver()
