-- Serving database: features for the ML application + audit tables for the pipeline.
-- Three accounts, each with only the rights it needs (least privilege):
--   pipeline_writer   owns the tables, used by ingestion and Airflow
--   ml_reader         read-only, features only          (the ML application)
--   dashboard_reader  read-only, features + audit tables (the dashboard)

CREATE ROLE pipeline_writer  LOGIN PASSWORD :'writer_pw';
CREATE ROLE ml_reader        LOGIN PASSWORD :'ml_pw';
CREATE ROLE dashboard_reader LOGIN PASSWORD :'dash_pw';

REVOKE ALL ON DATABASE serving FROM PUBLIC;
GRANT CONNECT ON DATABASE serving TO pipeline_writer, ml_reader, dashboard_reader;
REVOKE ALL ON SCHEMA public FROM PUBLIC;

CREATE SCHEMA ops      AUTHORIZATION pipeline_writer;
CREATE SCHEMA features AUTHORIZATION pipeline_writer;

SET ROLE pipeline_writer;

-- One row per log file copied into the raw zone (data lineage).
CREATE TABLE ops.ingested_files (
    file_name   text        PRIMARY KEY,
    month       char(7)     NOT NULL,
    sha256      char(64)    NOT NULL,
    line_count  bigint      NOT NULL,
    size_bytes  bigint      NOT NULL,
    hdfs_path   text        NOT NULL,
    ingested_at timestamptz NOT NULL DEFAULT now()
);

-- One row per pipeline step and period, success or failure (audit trail).
CREATE TABLE ops.pipeline_runs (
    run_id         bigserial   PRIMARY KEY,
    step           text        NOT NULL CHECK (step IN ('ingest', 'clean', 'quality_check', 'aggregate', 'deliver')),
    period         text        NOT NULL,   -- '2005-06' or '2005Q3'
    airflow_run_id text,
    rows_in        bigint,
    rows_out       bigint,
    rows_dropped   bigint,
    status         text        NOT NULL CHECK (status IN ('success', 'failed')),
    message        text,
    started_at     timestamptz NOT NULL,
    finished_at    timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX pipeline_runs_period ON ops.pipeline_runs (period, step);

-- The ML features: one row per midplane and hour.
CREATE TABLE features.midplane_hourly (
    quarter            char(6)   NOT NULL,   -- '2005Q3', the version of the data
    midplane           text      NOT NULL,   -- 'R02-M1'
    hour_start         timestamp NOT NULL,
    events_total       integer   NOT NULL,
    events_info        integer   NOT NULL,
    events_warning     integer   NOT NULL,
    events_error       integer   NOT NULL,
    events_fatal       integer   NOT NULL,
    kernel_events      integer   NOT NULL,
    app_events         integer   NOT NULL,
    distinct_templates integer   NOT NULL,
    alerts             integer   NOT NULL,
    alert_next_hour    boolean   NOT NULL,   -- the label the model learns to predict
    PRIMARY KEY (midplane, hour_start),
    CHECK (events_total >= alerts)
);
CREATE INDEX midplane_hourly_quarter ON features.midplane_hourly (quarter);

-- Simple entry point for the ML team: always the newest quarter.
CREATE VIEW features.latest_quarter AS
SELECT * FROM features.midplane_hourly
WHERE quarter = (SELECT max(quarter) FROM features.midplane_hourly);

RESET ROLE;

GRANT USAGE  ON SCHEMA features TO ml_reader, dashboard_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA features TO ml_reader, dashboard_reader;
GRANT USAGE  ON SCHEMA ops TO dashboard_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA ops TO dashboard_reader;
