"""Spark gateway: lets Airflow start a Spark job with ONE web request.

    POST http://spark-gateway:8000/run?job=clean_month&arg=2005-06
    POST http://spark-gateway:8000/run?job=build_features&arg=2005Q3

The request waits until the job has finished.
Answer: 200 + the job's report if it worked, 500 + the end of the log if it failed.
Only the two jobs below can be started (nothing else).
"""
import json
import re
import subprocess
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

JOBS = {"clean_month": "clean_month.py", "build_features": "build_features.py"}
JOB_DIR = "/opt/jobs"
SPARK_SUBMIT = "/opt/spark/bin/spark-submit"   # settings come from conf/spark-defaults.conf


def run_job(job: str, arg: str) -> tuple[int, dict]:
    if job not in JOBS or not re.fullmatch(r"\d{4}-\d{2}|\d{4}Q[1-4]", arg):
        return 400, {"error": "unknown job or bad argument"}
    command = [SPARK_SUBMIT, "--py-files", f"{JOB_DIR}/transform.py", f"{JOB_DIR}/{JOBS[job]}", arg]
    print("running:", " ".join(command), flush=True)
    done = subprocess.run(command, capture_output=True, text=True, cwd=JOB_DIR)

    # the job prints its report as the last line starting with "RESULT "
    results = [line[7:] for line in done.stdout.splitlines() if line.startswith("RESULT ")]
    report = json.loads(results[-1]) if results else {}
    if done.returncode != 0:
        return 500, {"error": f"{job} {arg} failed", "report": report, "log": done.stderr[-3000:]}
    return 200, report


class Handler(BaseHTTPRequestHandler):
    def reply(self, status: int, body: dict) -> None:
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps(body).encode())

    def do_GET(self):                       # health check for Docker
        self.reply(200, {"status": "ok"})

    def do_POST(self):                      # POST /run?job=...&arg=...
        query = parse_qs(urlparse(self.path).query)
        self.reply(*run_job(query.get("job", [""])[0], query.get("arg", [""])[0]))


if __name__ == "__main__":
    print("spark gateway listening on port 8000", flush=True)
    ThreadingHTTPServer(("0.0.0.0", 8000), Handler).serve_forever()
