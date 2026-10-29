#!/bin/bash
# Runs once, the first time the serving database starts with an empty volume.
# Passwords come from the environment (.env), never from Git.
set -euo pipefail

psql -v ON_ERROR_STOP=1 \
     --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
     -v writer_pw="$PIPELINE_WRITER_PASSWORD" \
     -v ml_pw="$ML_READER_PASSWORD" \
     -v dash_pw="$DASHBOARD_READER_PASSWORD" \
     -f /docker-entrypoint-initdb.d/sql/schema.sql
