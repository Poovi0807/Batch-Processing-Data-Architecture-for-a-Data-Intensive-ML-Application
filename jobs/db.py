"""Writing audit records and features to the serving database (PostgreSQL)."""
import os
from contextlib import contextmanager
from datetime import datetime, timezone

import psycopg2
from psycopg2.extras import execute_values


def connect():
    return psycopg2.connect(os.environ["SERVING_DB_DSN"])


def log_run(step, period, status, started_at, rows_in=None, rows_out=None, message=None):
    # 'dropped' means lost lines; aggregation turns events into fewer rows on purpose
    rows_dropped = None
    if step != "aggregate" and rows_in is not None and rows_out is not None:
        rows_dropped = rows_in - rows_out
    with connect() as conn, conn.cursor() as cur:
        cur.execute(
            """INSERT INTO ops.pipeline_runs
               (step, period, airflow_run_id, rows_in, rows_out, rows_dropped,
                status, message, started_at)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)""",
            (step, period, os.environ.get("AIRFLOW_CTX_DAG_RUN_ID"), rows_in, rows_out,
             rows_dropped, status, message, started_at),
        )


@contextmanager
def audited(step, period):
    """Record every step in ops.pipeline_runs, as success or as failure.

    with audited("clean", "2005-06") as record:
        record["rows_in"] = 100
        record["rows_out"] = 99
    """
    record = {"rows_in": None, "rows_out": None, "message": None}
    started_at = datetime.now(timezone.utc)
    status = "failed"
    try:
        yield record
        status = "success"
    except Exception as exc:
        record["message"] = str(exc)[:500]
        raise
    finally:
        log_run(step, period, status, started_at, **record)


FEATURE_COLUMNS = [
    "quarter", "midplane", "hour_start", "events_total", "events_info",
    "events_warning", "events_error", "events_fatal", "kernel_events",
    "app_events", "distinct_templates", "alerts", "alert_next_hour",
]


def replace_quarter(quarter, rows):
    """Delete the old version of a quarter and insert the new rows in ONE transaction.

    Running it twice gives the same result (idempotent), and the ML application
    never sees a half-loaded quarter.
    """
    with connect() as conn, conn.cursor() as cur:
        cur.execute("DELETE FROM features.midplane_hourly WHERE quarter = %s", (quarter,))
        execute_values(
            cur,
            f"INSERT INTO features.midplane_hourly ({', '.join(FEATURE_COLUMNS)}) VALUES %s",
            rows,
            page_size=5000,
        )
    return len(rows)
