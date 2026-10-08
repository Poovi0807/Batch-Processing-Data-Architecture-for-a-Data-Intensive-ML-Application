"""Ingestion microservice.

Small HTTP service (Python standard library only) that Airflow calls once a month:

    POST /ingest   {"year": 2005, "month": 6}
    GET  /health

For the requested month it
  1. finds the landing file bgl_YYYY-MM.log,
  2. validates it (file name, line format, date of each line) and computes a SHA-256 checksum,
  3. uploads it unchanged to the HDFS raw zone via WebHDFS
     (/data/raw/bgl/year=YYYY/month=MM/bgl_YYYY-MM.log, written to a temporary name and renamed,
     so a half-written file is never visible),
  4. records the load in the audit table governance.pipeline_runs.

Re-running the same month overwrites the same file, so retries are safe (idempotent).
"""
import hashlib
import json
import logging
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import psycopg
import requests

from bgl_format import LINE_RE, landing_name

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("ingest")

LANDING_DIR = Path(os.environ.get("LANDING_DIR", "/data/landing"))
WEBHDFS_URL = os.environ.get("WEBHDFS_URL", "http://hdfs-namenode:9870/webhdfs/v1")
HDFS_USER = os.environ.get("HDFS_USER", "etl")
RAW_ROOT = "/data/raw/bgl"
MAX_MALFORMED_RATIO = float(os.environ.get("MAX_MALFORMED_RATIO", "0.01"))

DB = dict(
    host=os.environ.get("SERVING_DB_HOST", "serving-db"),
    dbname=os.environ.get("SERVING_DB_NAME", "serving"),
    user=os.environ.get("SERVING_ETL_USER", "etl_writer"),
    password=os.environ.get("SERVING_ETL_PASSWORD", ""),
)


class IngestError(Exception):
    def __init__(self, status: int, message: str, details: dict | None = None):
        super().__init__(message)
        self.status, self.message, self.details = status, message, details or {}


# ------------------------------------------------------------------ validation
def validate(path: Path, year: int, month: int) -> dict:
    """Read the file once: count lines, check the format, compute the checksum."""
    sha = hashlib.sha256()
    lines = malformed = wrong_month = 0
    with open(path, "rb") as f:
        for raw in f:
            sha.update(raw)
            lines += 1
            m = LINE_RE.match(raw.decode("utf-8", errors="replace").rstrip("\n"))
            if not m:
                malformed += 1
            elif (int(m.group(3)), int(m.group(4))) != (year, month):
                wrong_month += 1
    if lines == 0:
        raise IngestError(422, "landing file is empty")
    stats = {
        "lines": lines,
        "malformed_lines": malformed,
        "wrong_month_lines": wrong_month,
        "malformed_ratio": round(malformed / lines, 6),
        "sha256": sha.hexdigest(),
        "bytes": path.stat().st_size,
    }
    if malformed / lines > MAX_MALFORMED_RATIO:
        raise IngestError(422, "too many malformed lines, file rejected", stats)
    return stats


# ------------------------------------------------------------------ WebHDFS
def _hdfs(method: str, path: str, op: str, **params):
    params = {"op": op, "user.name": HDFS_USER, **params}
    r = requests.request(method, f"{WEBHDFS_URL}{path}", params=params, allow_redirects=False, timeout=60)
    return r


def upload_to_hdfs(local: Path, target_dir: str, filename: str) -> str:
    r = _hdfs("PUT", target_dir, "MKDIRS", permission="750")
    r.raise_for_status()

    tmp_path, final_path = f"{target_dir}/_{filename}.tmp", f"{target_dir}/{filename}"
    # WebHDFS CREATE is a two-step call: the NameNode answers with a redirect to a DataNode
    r = _hdfs("PUT", tmp_path, "CREATE", overwrite="true", permission="640")
    if r.status_code != 307:
        raise IngestError(502, f"WebHDFS CREATE failed: {r.status_code} {r.text[:300]}")
    with open(local, "rb") as f:
        up = requests.put(r.headers["Location"], data=f, timeout=1800)
    if up.status_code != 201:
        raise IngestError(502, f"WebHDFS upload failed: {up.status_code} {up.text[:300]}")

    _hdfs("DELETE", final_path, "DELETE")  # replace an older load of the same month
    r = _hdfs("PUT", tmp_path, "RENAME", destination=final_path)
    r.raise_for_status()
    if not r.json().get("boolean"):
        raise IngestError(502, "WebHDFS RENAME failed")
    return final_path


# ------------------------------------------------------------------ audit
def audit(stage: str, partition: str, status: str, source: str, stats: dict) -> None:
    try:
        with psycopg.connect(**DB, connect_timeout=10) as conn:
            conn.execute(
                """INSERT INTO governance.pipeline_runs
                   (stage, partition_key, source_file, checksum_sha256, rows_in, rows_out,
                    rows_rejected, status, details)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                (stage, partition, source, stats.get("sha256"), stats.get("lines"),
                 stats.get("lines") if status == "success" else 0,
                 stats.get("malformed_lines"), status, json.dumps(stats)),
            )
    except Exception as exc:  # the audit must never hide the real result
        log.error("could not write audit record: %s", exc)


def ingest_month(year: int, month: int) -> dict:
    if not (1 <= month <= 12):
        raise IngestError(400, "month must be 1-12")
    name = landing_name(year, month)
    path = LANDING_DIR / name
    partition = f"{year:04d}-{month:02d}"
    if not path.is_file():
        raise IngestError(404, f"landing file {name} not found (did you run prepare_data.py?)")

    log.info("validating %s", path)
    try:
        stats = validate(path, year, month)
    except IngestError as e:
        audit("ingest", partition, "rejected", name, e.details)
        raise
    target_dir = f"{RAW_ROOT}/year={year:04d}/month={month:02d}"
    log.info("uploading %s (%s lines) to hdfs:%s", name, f"{stats['lines']:,}", target_dir)
    hdfs_path = upload_to_hdfs(path, target_dir, name)
    audit("ingest", partition, "success", name, stats)
    return {"status": "success", "partition": partition, "hdfs_path": hdfs_path, **stats}


# ------------------------------------------------------------------ HTTP
class Handler(BaseHTTPRequestHandler):
    def _send(self, status: int, body: dict) -> None:
        data = json.dumps(body).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        if self.path == "/health":
            self._send(200, {"status": "ok"})
        else:
            self._send(404, {"error": "not found"})

    def do_POST(self):
        if self.path != "/ingest":
            return self._send(404, {"error": "not found"})
        try:
            length = int(self.headers.get("Content-Length", 0))
            req = json.loads(self.rfile.read(length) or b"{}")
            result = ingest_month(int(req["year"]), int(req["month"]))
            self._send(200, result)
        except IngestError as e:
            log.warning("ingest failed: %s", e.message)
            self._send(e.status, {"status": "failed", "error": e.message, **e.details})
        except (KeyError, ValueError, json.JSONDecodeError):
            self._send(400, {"status": "failed", "error": 'body must be {"year": YYYY, "month": M}'})
        except Exception as e:
            log.exception("unexpected error")
            self._send(500, {"status": "failed", "error": str(e)})

    def log_message(self, fmt, *args):
        log.info("%s %s", self.address_string(), fmt % args)


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "8000"))
    log.info("ingest service listening on :%s, landing dir %s", port, LANDING_DIR)
    ThreadingHTTPServer(("0.0.0.0", port), Handler).serve_forever()
