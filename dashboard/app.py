"""Pipeline dashboard: shows where the data is.   ->  http://localhost:8501

It only READS: the landing folder, HDFS (WebHDFS) and the audit + feature tables in PostgreSQL.
"""
import os
from pathlib import Path

import pandas as pd
import psycopg
import requests
import streamlit as st

LANDING = Path(os.environ.get("LANDING_DIR", "/data/landing"))
WEBHDFS = os.environ.get("WEBHDFS_URL", "http://hdfs-namenode:9870/webhdfs/v1")
DB = dict(host=os.environ.get("SERVING_DB_HOST", "serving-db"), port=int(os.environ.get("SERVING_DB_PORT", "5432")),
          dbname="serving", user="etl_writer", password=os.environ.get("ETL_PASSWORD", ""))

st.set_page_config(page_title="BGL pipeline", layout="wide")


def in_hdfs(path: str) -> bool:
    """True if the path exists in HDFS."""
    try:
        r = requests.get(WEBHDFS + path, params={"op": "GETFILESTATUS", "user.name": "etl"}, timeout=5)
        return r.status_code == 200
    except requests.RequestException:
        return False


def sql(query: str) -> pd.DataFrame:
    try:
        with psycopg.connect(**DB, connect_timeout=5) as conn:
            cur = conn.execute(query)
            return pd.DataFrame(cur.fetchall(), columns=[c.name for c in cur.description])
    except psycopg.Error:
        return pd.DataFrame()


st.title("BGL batch pipeline – where is my data?")
st.write("Data moves from left to right: **① landing folder → ② HDFS raw → ③ HDFS processed "
         "→ ④ HDFS curated → ⑤ PostgreSQL** (read by the ML application). "
         "[Airflow](http://localhost:8080) runs the steps; this page refreshes every 20 seconds.")


@st.fragment(run_every="20s")
def show_status():
    months = sorted(f.name[4:11] for f in LANDING.glob("bgl_*.log"))          # bgl_2005-06.log -> 2005-06
    quarters = sorted({f"{m[:4]}Q{(int(m[5:]) - 1) // 3 + 1}" for m in months})
    delivered = sql("SELECT quarter, count(*) AS rows, count(*) FILTER (WHERE alert_next_hour) AS labels "
                    "FROM features.midplane_hourly GROUP BY quarter")
    rows_of = dict(zip(delivered.get("quarter", []), delivered.get("rows", [])))

    month_table = pd.DataFrame({
        "Month": months,
        "① Landing file": "✅",
        "② HDFS raw": ["✅" if in_hdfs(f"/data/raw/bgl/month={m}") else "⏳" for m in months],
        "③ HDFS processed": ["✅" if in_hdfs(f"/data/processed/bgl/month={m}/_SUCCESS") else "⏳" for m in months],
    })
    quarter_table = pd.DataFrame({
        "Quarter": quarters,
        "④ HDFS curated": ["✅" if in_hdfs(f"/data/curated/features/quarter={q}/_SUCCESS") else "⏳" for q in quarters],
        "⑤ PostgreSQL rows": [f"✅ {rows_of[q]:,}" if q in rows_of else "⏳" for q in quarters],
    })

    done = lambda col: int((col.astype(str).str.startswith("✅")).sum())   # noqa: E731
    c = st.columns(5)
    c[0].metric("① Landing (months)", len(months))
    c[1].metric("② HDFS raw", f"{done(month_table['② HDFS raw'])} / {len(months)}")
    c[2].metric("③ HDFS processed", f"{done(month_table['③ HDFS processed'])} / {len(months)}")
    c[3].metric("④ HDFS curated", f"{done(quarter_table['④ HDFS curated'])} / {len(quarters)}")
    c[4].metric("⑤ PostgreSQL", f"{done(quarter_table['⑤ PostgreSQL rows'])} / {len(quarters)}")

    left, right = st.columns(2)
    left.subheader("Months (ingest → clean)")
    left.dataframe(month_table, hide_index=True, width="stretch")
    right.subheader("Quarters (features → delivery)")
    right.dataframe(quarter_table, hide_index=True, width="stretch")

    st.subheader("Audit log – the last steps of the pipeline")
    st.dataframe(sql("SELECT run_time, stage, partition_key, status, rows_in, rows_out "
                     "FROM governance.pipeline_runs ORDER BY id DESC LIMIT 20"), hide_index=True, width="stretch")
    if not months:
        st.info("No landing files yet – run:  docker compose run --rm ingest python prepare_data.py")


show_status()
