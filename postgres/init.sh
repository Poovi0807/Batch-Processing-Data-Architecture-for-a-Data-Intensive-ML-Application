#!/bin/bash
# Runs ONCE, when the serving database is created for the first time.
# Creates the feature table, the audit table and two users:
#   etl_writer - used by the pipeline: writes features and audit lines
#   ml_reader  - used by the ML application: may only READ the features
set -e
psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" \
     -v etl_pw="$ETL_PASSWORD" -v ml_pw="$ML_PASSWORD" <<'SQL'

CREATE ROLE etl_writer LOGIN PASSWORD :'etl_pw';
CREATE ROLE ml_reader  LOGIN PASSWORD :'ml_pw';
REVOKE ALL ON DATABASE serving FROM PUBLIC;
GRANT CONNECT ON DATABASE serving TO etl_writer, ml_reader;

-- what the ML application reads: one row per midplane and hour, one version per quarter
CREATE SCHEMA features;
CREATE TABLE features.midplane_hourly (
    quarter             text        NOT NULL,   -- e.g. 2005Q3
    midplane            text        NOT NULL,   -- e.g. R02-M1
    hour_start          timestamptz NOT NULL,
    event_count         integer, cnt_info integer, cnt_warning integer, cnt_severe integer,
    cnt_error integer, cnt_fatal integer, cnt_failure integer,
    cnt_kernel integer, cnt_app integer, cnt_mmcs integer, cnt_discovery integer,
    cnt_monitor integer, cnt_hardware integer, cnt_other_component integer,
    distinct_templates  integer,
    distinct_nodes      integer,
    alert_count         integer,
    alert_next_hour     boolean     NOT NULL,   -- the label the ML model learns
    PRIMARY KEY (quarter, midplane, hour_start)
);

-- audit table: every pipeline step writes one line (who did what, how many rows, result)
CREATE SCHEMA governance;
CREATE TABLE governance.pipeline_runs (
    id             serial      PRIMARY KEY,
    run_time       timestamptz NOT NULL DEFAULT now(),
    stage          text        NOT NULL,       -- ingest | clean | features
    partition_key  text        NOT NULL,       -- 2005-06 or 2005Q3
    status         text        NOT NULL,       -- success | failed
    rows_in        bigint,
    rows_out       bigint,
    details        jsonb
);

-- rights (least privilege)
GRANT USAGE ON SCHEMA features, governance TO etl_writer;
GRANT SELECT, INSERT, DELETE ON features.midplane_hourly TO etl_writer;
GRANT SELECT, INSERT ON governance.pipeline_runs TO etl_writer;
GRANT USAGE ON SEQUENCE governance.pipeline_runs_id_seq TO etl_writer;
GRANT USAGE ON SCHEMA features TO ml_reader;
GRANT SELECT ON features.midplane_hourly TO ml_reader;
SQL
