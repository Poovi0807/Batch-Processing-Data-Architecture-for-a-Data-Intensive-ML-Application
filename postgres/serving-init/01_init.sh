#!/bin/bash
# Runs once, when the serving database volume is created.
# Creates the schemas, tables and the two technical users with least privilege:
#   etl_writer - used by the pipeline (ingest + Spark): writes features and audit records
#   ml_reader  - used by the ML application: read-only access to the feature schema only
set -euo pipefail

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
     -v etl_pw="$SERVING_ETL_PASSWORD" -v ml_pw="$SERVING_ML_PASSWORD" <<'SQL'

CREATE ROLE etl_writer LOGIN PASSWORD :'etl_pw';
CREATE ROLE ml_reader  LOGIN PASSWORD :'ml_pw';

REVOKE ALL ON DATABASE serving FROM PUBLIC;
GRANT CONNECT ON DATABASE serving TO etl_writer, ml_reader;
REVOKE CREATE ON SCHEMA public FROM PUBLIC;

-- ---------------------------------------------------------------- delivery
CREATE SCHEMA features;

CREATE TABLE features.midplane_hourly (
    quarter              text        NOT NULL,   -- version of the feature set, e.g. 2005Q3
    midplane             text        NOT NULL,   -- e.g. R02-M1
    hour_start           timestamptz NOT NULL,
    event_count          integer     NOT NULL,
    cnt_info             integer     NOT NULL,
    cnt_warning          integer     NOT NULL,
    cnt_severe           integer     NOT NULL,
    cnt_error            integer     NOT NULL,
    cnt_fatal            integer     NOT NULL,
    cnt_failure          integer     NOT NULL,
    cnt_kernel           integer     NOT NULL,
    cnt_app              integer     NOT NULL,
    cnt_mmcs             integer     NOT NULL,
    cnt_discovery        integer     NOT NULL,
    cnt_monitor          integer     NOT NULL,
    cnt_hardware         integer     NOT NULL,
    cnt_other_component  integer     NOT NULL,
    distinct_templates   integer     NOT NULL,
    distinct_nodes       integer     NOT NULL,
    alert_count          integer     NOT NULL,
    alert_next_hour      boolean     NOT NULL,   -- ML label
    loaded_at            timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (quarter, midplane, hour_start)
);

COMMENT ON TABLE features.midplane_hourly IS
  'Hourly features per BGL midplane; one version per quarter. Label: alert_next_hour.';

-- convenience view: the most recent quarter only
CREATE VIEW features.midplane_hourly_latest AS
SELECT * FROM features.midplane_hourly
WHERE quarter = (SELECT max(quarter) FROM features.midplane_hourly);

-- ---------------------------------------------------------------- governance
CREATE SCHEMA governance;

CREATE TABLE governance.pipeline_runs (
    id               bigserial   PRIMARY KEY,
    run_ts           timestamptz NOT NULL DEFAULT now(),
    stage            text        NOT NULL,   -- ingest | parse_clean | aggregate_deliver
    partition_key    text        NOT NULL,   -- 2005-06 or 2005Q3
    source_file      text,
    checksum_sha256  text,
    rows_in          bigint,
    rows_out         bigint,
    rows_rejected    bigint,
    status           text        NOT NULL,   -- success | rejected | failed
    details          jsonb
);

-- ---------------------------------------------------------------- privileges
GRANT USAGE ON SCHEMA features, governance TO etl_writer;
GRANT SELECT, INSERT, DELETE ON features.midplane_hourly TO etl_writer;
GRANT SELECT, INSERT ON governance.pipeline_runs TO etl_writer;
GRANT USAGE ON SEQUENCE governance.pipeline_runs_id_seq TO etl_writer;

GRANT USAGE ON SCHEMA features TO ml_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA features TO ml_reader;   -- incl. the view
-- ml_reader gets no access to the governance schema

SQL
echo "serving database initialised"
