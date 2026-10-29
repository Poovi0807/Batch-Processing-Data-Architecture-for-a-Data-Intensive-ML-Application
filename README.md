# BGL batch pipeline

A batch-processing data system for IT-operations logs, built as Docker microservices.
It ingests the [BGL supercomputer log](https://github.com/logpai/loghub/tree/master/BGL)
(4.7 million lines, June 2005 – January 2006) month by month, cleans it with Spark,
aggregates it every quarter into hourly features per midplane, and delivers them to
PostgreSQL for a machine-learning application that predicts alerts.

```
landing folder ──► ingest ──► HDFS raw ──► Spark clean ──► HDFS processed
                                                              │ (quarterly)
     ML application ◄── PostgreSQL ◄── deliver ◄── HDFS curated ◄── Spark aggregate
                           ▲
                        dashboard            Airflow schedules every step
```

| Layer | Technology |
|---|---|
| Orchestration | Apache Airflow 3.3.2 |
| Storage | HDFS 3.4.3 (1 NameNode, 2 DataNodes, replication 2) |
| Processing | Apache Spark 4.2.0 (master, 2 workers, Spark Connect) |
| Delivery | PostgreSQL 17 |
| Ingestion | Flask service |
| User interface | Streamlit dashboard |

## Quick start

Needs Docker with at least 8 GB of memory and Python 3.10+.

```bash
git clone https://github.com/Poovi0807/Batch-Processing-Data-Architecture-for-a-Data-Intensive-ML-Application.git
cd Batch-Processing-Data-Architecture-for-a-Data-Intensive-ML-Application
notepad .env                              # create .env with your passwords
bash scripts/download_data.sh             # Windows: powershell -ExecutionPolicy Bypass -File scripts\download_data.ps1
docker compose up -d --build              # first start takes 10-20 minutes
```

The download script fetches `BGL.zip` (57.5 MB) from the official Loghub record on
[Zenodo](https://zenodo.org/records/8196385) and splits it into
`data/landing/BGL_2005-06.log` … `BGL_2006-01.log` (4,747,963 lines in total).
If you already have `BGL.log`, run `python scripts/split_by_month.py path/to/BGL.log` instead.

1. Open Airflow at <http://localhost:8080> and log in with the admin user from `.env`.
2. Switch on the DAG **bgl_monthly_ingest_clean**. Airflow replays all eight months and
   starts **bgl_quarterly_aggregate_deliver** after each quarter by itself.
3. Open the dashboard at <http://localhost:8501>.

Stop with `docker compose down` (data is kept) or `docker compose down -v` (everything deleted).

## Troubleshooting

| Symptom | Fix |
|---|---|
| A container stops with exit code 137 | Give Docker at least 8 GB of memory |
| `port is already allocated` | Another program uses the port: stop it, or change the left number in `ports:` |
| The downloaded zip is empty (a few bytes) | Download again; the script stops with a message if this happens |
| Strange file errors inside a OneDrive folder | Pause OneDrive sync, or keep the project outside OneDrive (e.g. `C:\bgl`) |
| Airflow login fails after changing `.env` | The admin user is created once: `docker compose down -v`, then start again |
| `ingest` task fails with 404 / 409 | Monthly file missing / a different file for an already ingested month (intended protection) |

Every push runs the unit tests and checks `docker-compose.yml` with GitHub Actions
(`.github/workflows/ci.yml`).

## Web interfaces

| What | Address |
|---|---|
| Airflow | http://localhost:8080 |
| Dashboard | http://localhost:8501 |
| HDFS NameNode | http://localhost:9870 |
| Spark master | http://localhost:8081 |
| Serving database (ML) | `localhost:5433`, database `serving`, user `ml_reader` |

## Repository layout

| Folder | Content |
|---|---|
| `docker-compose.yml` | all 16 services, networks, volumes, health checks |
| `airflow/` | Airflow image and the two DAGs |
| `jobs/` | pipeline code: Spark transformations, one function per step, database helpers |
| `ingest/` | ingestion microservice |
| `dashboard/` | Streamlit user interface |
| `hadoop/` | HDFS configuration and zone set-up |
| `postgres/init/` | schema, roles and rights of the serving database |
| `scripts/` | split the dataset by month, download it, generate load-test data |
| `tests/` | 22 unit tests on the real 2,000-line BGL sample |

## Tests

```bash
pip install -r tests/requirements.txt
pytest                     # Spark tests need Java 17+, they are skipped without it
```

## Data source

Loghub BGL dataset, free for research use. Zhu et al., *Loghub: A Large Collection of
System Log Datasets for AI-driven Log Analytics*, ISSRE 2023.
