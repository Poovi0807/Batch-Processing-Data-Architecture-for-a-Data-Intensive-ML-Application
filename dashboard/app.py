"""Pipeline dashboard (Streamlit).

Three pages:
  1. Pipeline status   - is the data there, did every step succeed?
  2. Explore features  - the hourly features the ML application will read
  3. How to use        - the steps to run the pipeline and the links to every UI

It connects with the read-only account 'dashboard_reader'.
"""
import os

import pandas as pd
import psycopg2
import streamlit as st

DSN = os.environ["DASHBOARD_DB_DSN"]
STEPS = ["ingest", "clean", "quality_check", "aggregate", "deliver"]

st.set_page_config(page_title="BGL Pipeline Dashboard", page_icon=":bar_chart:", layout="wide")


@st.cache_data(ttl=30)
def query(sql: str, params: tuple = ()) -> pd.DataFrame:
    with psycopg2.connect(DSN) as conn, conn.cursor() as cur:
        cur.execute(sql, params)
        columns = [c.name for c in cur.description]
        return pd.DataFrame(cur.fetchall(), columns=columns)


def database_ready() -> bool:
    try:
        query("SELECT 1")
        return True
    except psycopg2.Error as exc:
        st.error(f"Cannot reach the serving database yet. Is the stack running?\n\n{exc}")
        return False


# ---------------- page 1 ----------------
def page_status():
    st.title("Pipeline status")
    st.caption("Monthly: ingest → clean → quality check. Quarterly: aggregate → deliver.")

    files = query("SELECT count(*) AS files, coalesce(sum(line_count), 0) AS lines FROM ops.ingested_files")
    features = query("SELECT count(*) AS rows, count(DISTINCT quarter) AS quarters FROM features.midplane_hourly")
    last = query("SELECT max(finished_at) AS last FROM ops.pipeline_runs WHERE status = 'success'")
    failed = query("SELECT count(*) AS n FROM ops.pipeline_runs WHERE status = 'failed'")

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Monthly files ingested", int(files.files[0]))
    c2.metric("Raw log lines", f"{int(files.lines[0]):,}")
    c3.metric("Feature rows delivered", f"{int(features.rows[0]):,}",
              help=f"{int(features.quarters[0])} quarter(s) available to the ML application")
    last_run = last["last"][0]
    c4.metric("Last successful step", last_run.strftime("%d %b, %H:%M") if pd.notna(last_run) else "none yet",
              help="Time in UTC")

    if int(files.files[0]) == 0:
        st.info("No data yet. Open Airflow (http://localhost:8080), switch on the DAG "
                "**bgl_monthly_ingest_clean** and wait for the first month to finish. "
                "See the page **How to use** for all steps.")
        return

    if int(failed.n[0]) > 0:
        st.warning(f"{int(failed.n[0])} step(s) failed at least once. Airflow retries them; "
                   "the table below shows the latest result per step.")

    st.subheader("Latest result per period and step")
    latest = query("""
        SELECT DISTINCT ON (period, step) period, step, status
        FROM ops.pipeline_runs ORDER BY period, step, finished_at DESC""")
    grid = latest.pivot(index="period", columns="step", values="status")
    grid = grid.reindex(columns=[s for s in STEPS if s in grid.columns])
    st.dataframe(grid.replace({"success": "✅ success", "failed": "❌ failed"}).fillna("–"),
                 use_container_width=True)

    st.subheader("Lines per month: raw vs. after cleaning")
    cleaned = query("""
        SELECT DISTINCT ON (period) period AS month, rows_in AS raw_lines, rows_out AS clean_rows
        FROM ops.pipeline_runs WHERE step = 'clean' AND status = 'success'
        ORDER BY period, finished_at DESC""")
    if not cleaned.empty:
        st.bar_chart(cleaned.set_index("month")[["raw_lines", "clean_rows"]], stack=False)
        cleaned["dropped"] = cleaned.raw_lines - cleaned.clean_rows
        cleaned["drop rate"] = (cleaned.dropped / cleaned.raw_lines).map("{:.3%}".format)
        st.dataframe(cleaned, hide_index=True, use_container_width=True)

    st.subheader("Recent runs")
    runs = query("""
        SELECT finished_at, step, period, status, rows_in, rows_out, message
        FROM ops.pipeline_runs ORDER BY finished_at DESC LIMIT 25""")
    st.dataframe(runs, hide_index=True, use_container_width=True)


# ---------------- page 2 ----------------
def page_features():
    st.title("Explore features")
    st.caption("One row per midplane and hour. The column alert_next_hour is the label the model learns.")

    quarters = query("SELECT DISTINCT quarter FROM features.midplane_hourly ORDER BY quarter")
    if quarters.empty:
        st.info("No features yet. They appear after the quarterly DAG has run.")
        return

    left, right = st.columns(2)
    quarter = left.selectbox("Quarter", quarters.quarter.tolist(), index=len(quarters) - 1)
    ranking = query("""
        SELECT midplane, sum(alerts) AS alerts, sum(events_total) AS events
        FROM features.midplane_hourly WHERE quarter = %s
        GROUP BY midplane ORDER BY alerts DESC, events DESC""", (quarter,))
    labels = [f"{r.midplane}  ({int(r.alerts):,} alerts)" for r in ranking.itertuples()]
    choice = right.selectbox("Midplane (most alerts first)", labels)
    midplane = choice.split()[0]

    rows = query("""
        SELECT * FROM features.midplane_hourly
        WHERE quarter = %s AND midplane = %s ORDER BY hour_start""", (quarter, midplane))

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Hours with events", f"{len(rows):,}")
    c2.metric("Events", f"{int(rows.events_total.sum()):,}")
    c3.metric("Alert lines", f"{int(rows.alerts.sum()):,}")
    c4.metric("Hours followed by an alert", f"{rows.alert_next_hour.mean():.1%}")

    st.subheader(f"Events per hour on {midplane}")
    st.caption("Only hours with at least one event are shown.")
    st.bar_chart(rows.set_index("hour_start")[["events_total", "alerts"]], stack=False)

    st.subheader(f"Midplanes with the most alerts in {quarter}")
    st.bar_chart(ranking.head(15).set_index("midplane")["alerts"])

    st.subheader("Feature rows")
    st.dataframe(rows, hide_index=True, use_container_width=True)
    st.download_button("Download these rows as CSV", rows.to_csv(index=False).encode(),
                       file_name=f"features_{quarter}_{midplane}.csv", mime="text/csv")


# ---------------- page 3 ----------------
def page_help():
    st.title("How to use")
    st.markdown("""
**Run the pipeline**

1. Put the monthly files `BGL_2005-06.log` … `BGL_2006-01.log` into `data/landing/`
   (`python scripts/split_by_month.py BGL.log` creates them).
2. Start everything: `docker compose up -d --build`
3. Open **Airflow** at [localhost:8080](http://localhost:8080) and log in with the
   user from your `.env` file.
4. Switch on **bgl_monthly_ingest_clean**. Airflow replays all eight months one after
   the other and starts **bgl_quarterly_aggregate_deliver** after each quarter.
5. Come back to this dashboard and refresh.

**Other user interfaces**

| What | Address |
|---|---|
| Airflow (runs, logs, retries) | [localhost:8080](http://localhost:8080) |
| HDFS NameNode (files, DataNodes) | [localhost:9870](http://localhost:9870) |
| Spark master (workers, jobs) | [localhost:8081](http://localhost:8081) |
| This dashboard | [localhost:8501](http://localhost:8501) |

**For the ML application**

Connect to PostgreSQL on `localhost:5433`, database `serving`, user `ml_reader`
(read-only). The newest data is in the view `features.latest_quarter`:

```python
import pandas as pd, psycopg2
conn = psycopg2.connect("host=localhost port=5433 dbname=serving user=ml_reader password=...")
df = pd.read_sql("SELECT * FROM features.latest_quarter", conn)
```
""")


PAGES = {"1 · Pipeline status": page_status, "2 · Explore features": page_features, "3 · How to use": page_help}

st.sidebar.title("BGL batch pipeline")
st.sidebar.caption("IT operations logs → ML features")
page = st.sidebar.radio("Page", list(PAGES))
if st.sidebar.button("Refresh data"):
    st.cache_data.clear()

if page.startswith("3") or database_ready():
    PAGES[page]()
