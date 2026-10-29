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
git clone <this repository> && cd bgl-batch-pipeline
cp .env.example .env                      # then change every password in .env
python scripts/split_by_month.py path/to/BGL.log   # writes data/landing/BGL_YYYY-MM.log
docker compose up -d --build              # first start takes a few minutes
```

1. Open Airflow at <http://localhost:8080> and log in with the admin user from `.env`.
2. Switch on the DAG **bgl_monthly_ingest_clean**. Airflow replays all eight months and
   starts **bgl_quarterly_aggregate_deliver** after each quarter by itself.
3. Open the dashboard at <http://localhost:8501>.

Stop with `docker compose down` (data is kept) or `docker compose down -v` (everything deleted).

The dataset is on Kaggle as `omduggineni/loghub-bgl-log-data`
(`scripts/download_data.sh` downloads and splits it if you have a Kaggle API key).

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
