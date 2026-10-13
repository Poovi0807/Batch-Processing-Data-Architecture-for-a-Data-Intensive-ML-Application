"""Ingestion service: copies ONE monthly log file from the landing folder into the HDFS raw zone.

Airflow calls it once per month:
    POST http://ingest:8000/ingest?month=2005-06

It answers with a small report, for example:
    {"month": "2005-06", "lines": 1240932, "broken_lines": 0, "sha256": "...", "hdfs_path": "..."}
"""
import hashlib
import json
import os
import re
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

import requests

LANDING = os.environ.get("LANDING_DIR", "/data/landing")
WEBHDFS = os.environ.get("WEBHDFS_URL", "http://hdfs-namenode:9870/webhdfs/v1")
HDFS_USER = "etl"           # technical user that owns the data lake
MAX_BROKEN = 0.01           # reject the file if more than 1 % of the lines are broken

# a correct BGL line has 9 fields separated by spaces, then the message
GOOD_LINE = re.compile(r"^\S+ \d+ \d{4}\.\d{2}\.\d{2} \S+ \S+ \S+ \S+ \S+ \S+")


def check_file(path: str) -> dict:
    """Read the file once: count all lines and broken lines, compute a SHA-256 fingerprint."""
    sha, lines, broken = hashlib.sha256(), 0, 0
    with open(path, "rb") as f:
        for raw in f:
            sha.update(raw)
            lines += 1
            if not GOOD_LINE.match(raw.decode("utf-8", errors="replace")):
                broken += 1
    return {"lines": lines, "broken_lines": broken, "sha256": sha.hexdigest()}


def upload_to_hdfs(local_path: str, hdfs_path: str) -> None:
    """Upload a file with the WebHDFS REST API (two calls: the NameNode answers with
    the address of a DataNode, then the data is sent to that DataNode)."""
    params = {"op": "CREATE", "overwrite": "true", "user.name": HDFS_USER}
    answer = requests.put(WEBHDFS + hdfs_path, params=params, allow_redirects=False, timeout=60)
    datanode_url = answer.headers["Location"]
    with open(local_path, "rb") as f:
        requests.put(datanode_url, data=f, timeout=3600).raise_for_status()


def ingest(month: str) -> dict:
    if not re.fullmatch(r"\d{4}-\d{2}", month):
        raise ValueError("month must look like 2005-06")
    local_path = f"{LANDING}/bgl_{month}.log"
    if not os.path.exists(local_path):
        raise FileNotFoundError(f"{local_path} not found - run prepare_data.py first")

    report = check_file(local_path)
    if report["broken_lines"] > MAX_BROKEN * report["lines"]:
        raise ValueError(f"file rejected, too many broken lines: {report}")

    hdfs_path = f"/data/raw/bgl/month={month}/bgl_{month}.log"
    upload_to_hdfs(local_path, hdfs_path)
    print(f"ingested {month}: {report['lines']:,} lines -> {hdfs_path}", flush=True)
    return {"month": month, "hdfs_path": hdfs_path, **report}


class Handler(BaseHTTPRequestHandler):
    def reply(self, status: int, body: dict) -> None:
        data = json.dumps(body).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):                       # health check for Docker
        self.reply(200, {"status": "ok"})

    def do_POST(self):                      # POST /ingest?month=2005-06
        url = urlparse(self.path)
        month = parse_qs(url.query).get("month", [""])[0]
        try:
            self.reply(200, ingest(month))
        except Exception as error:
            self.reply(500, {"error": str(error)})


if __name__ == "__main__":
    print("ingest service listening on port 8000", flush=True)
    ThreadingHTTPServer(("0.0.0.0", 8000), Handler).serve_forever()
