"""Ingestion service: copies one monthly BGL log file from the landing folder
into the raw zone of the data lake (HDFS) and records it in ops.ingested_files.

Endpoints
  GET  /health          -> {"status": "ok"}
  GET  /files           -> files in the landing folder and whether they are ingested
  POST /ingest          -> body {"month": "2005-06"}

Rules
  * the file must exist and look like BGL log lines (otherwise 422)
  * the same file twice is fine: it is skipped ("already_ingested")
  * a DIFFERENT file for a month that is already ingested is refused (409):
    the raw zone is never overwritten
"""
import hashlib
import os
import re
import shutil
from pathlib import Path

import psycopg2
from flask import Flask, jsonify, request
from hdfs import InsecureClient

LANDING_DIR = Path(os.environ.get("LANDING_DIR", "/landing"))
RAW_BASE = os.environ.get("RAW_BASE", "/bgl/raw")
HDFS_URL = os.environ.get("HDFS_URL", "http://namenode:9870")
HDFS_USER = os.environ.get("HDFS_USER", "pipeline")
# "hdfs" in the containers; a local folder only for tests without HDFS
RAW_TARGET = os.environ.get("RAW_TARGET", "hdfs")

MONTH_PATTERN = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")
LINE_START = re.compile(rb"^\S+ \d+ \d{4}\.\d{2}\.\d{2} ")
SAMPLE_LINES = 10_000          # lines checked for the BGL format
MIN_VALID_SHARE = 0.95         # at least 95 % of them must look like BGL lines

app = Flask(__name__)


def file_name(month: str) -> str:
    return f"BGL_{month}.log"


def inspect_file(path: Path) -> dict:
    """Checksum, size and line count, plus a format check on the first lines."""
    sha256, lines, valid = hashlib.sha256(), 0, 0
    with path.open("rb") as handle:
        for line in handle:
            sha256.update(line)
            if lines < SAMPLE_LINES and LINE_START.match(line):
                valid += 1
            lines += 1
    checked = min(lines, SAMPLE_LINES)
    return {
        "sha256": sha256.hexdigest(),
        "line_count": lines,
        "size_bytes": path.stat().st_size,
        "valid_share": valid / checked if checked else 0.0,
    }


def store(path: Path, month: str) -> str:
    """Copy the file into the raw zone and check that it arrived complete."""
    target = f"{RAW_BASE}/month={month}/{path.name}"
    if RAW_TARGET == "hdfs":
        client = InsecureClient(HDFS_URL, user=HDFS_USER)
        client.makedirs(f"{RAW_BASE}/month={month}")
        client.upload(target, str(path), overwrite=True)
        stored_size = client.status(target)["length"]
    else:
        local_target = Path(RAW_TARGET) / f"month={month}" / path.name
        local_target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(path, local_target)
        stored_size = local_target.stat().st_size
    if stored_size != path.stat().st_size:
        raise IOError(f"Upload incomplete: {stored_size} of {path.stat().st_size} bytes")
    return target


def db():
    return psycopg2.connect(os.environ["SERVING_DB_DSN"])


def already_ingested(name: str):
    with db() as conn, conn.cursor() as cur:
        cur.execute("SELECT sha256 FROM ops.ingested_files WHERE file_name = %s", (name,))
        row = cur.fetchone()
    return row[0] if row else None


def record(name, month, info, hdfs_path):
    with db() as conn, conn.cursor() as cur:
        cur.execute(
            """INSERT INTO ops.ingested_files
               (file_name, month, sha256, line_count, size_bytes, hdfs_path)
               VALUES (%s, %s, %s, %s, %s, %s)""",
            (name, month, info["sha256"], info["line_count"], info["size_bytes"], hdfs_path),
        )


@app.get("/health")
def health():
    return jsonify(status="ok")


@app.get("/files")
def files():
    names = sorted(p.name for p in LANDING_DIR.glob("BGL_*.log"))
    return jsonify([{"file": n, "ingested": already_ingested(n) is not None} for n in names])


@app.post("/ingest")
def ingest():
    month = (request.get_json(silent=True) or {}).get("month", "")
    if not MONTH_PATTERN.match(month):
        return jsonify(error="month must look like 2005-06"), 400

    path = LANDING_DIR / file_name(month)
    if not path.is_file():
        return jsonify(error=f"{path.name} not found in the landing folder"), 404

    info = inspect_file(path)
    if info["line_count"] == 0 or info["valid_share"] < MIN_VALID_SHARE:
        return jsonify(error=f"{path.name} does not look like a BGL log file", **info), 422

    known_sha = already_ingested(path.name)
    if known_sha == info["sha256"]:
        return jsonify(status="already_ingested", month=month, **info)
    if known_sha is not None:
        return jsonify(error="a different file for this month is already in the raw zone; "
                             "the raw zone is never overwritten"), 409

    hdfs_path = store(path, month)
    record(path.name, month, info, hdfs_path)
    app.logger.info("Ingested %s (%s lines) to %s", path.name, info["line_count"], hdfs_path)
    return jsonify(status="ingested", month=month, hdfs_path=hdfs_path, **info), 201
