"""Spark job gateway (microservice).

Airflow does not need Spark or Java installed: it asks this service to run a job
and polls for the result.

    POST /jobs        {"job": "parse_clean_month", "args": [2005, 6]}  -> {"job_id": "..."}
    GET  /jobs/<id>   -> {"status": "running|succeeded|failed", "returncode": .., "log_tail": ".."}
    GET  /health

Only the jobs listed in JOBS can be started (no arbitrary commands).
The Spark driver runs in this container; executors run on the Spark workers.
"""
import json
import logging
import os
import subprocess
import threading
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("spark-gateway")

JOB_DIR = Path("/opt/pipeline/jobs")
LOG_DIR = Path("/var/spark-jobs")
JOBS = {  # job name -> (script, number of integer arguments)
    "parse_clean_month": ("parse_clean_month.py", 2),
    "aggregate_quarter": ("aggregate_quarter.py", 2),
}

_runs: dict[str, dict] = {}
_lock = threading.Lock()


def spark_submit_cmd(script: str, args: list[int]) -> list[str]:
    env = os.environ
    return [
        "/opt/spark/bin/spark-submit",
        "--master", env.get("SPARK_MASTER_URL", "spark://spark-master:7077"),
        "--deploy-mode", "client",
        "--conf", "spark.driver.host=spark-gateway",
        "--conf", "spark.driver.bindAddress=0.0.0.0",
        "--conf", f"spark.driver.memory={env.get('SPARK_DRIVER_MEMORY', '1g')}",
        "--conf", f"spark.executor.memory={env.get('SPARK_EXECUTOR_MEMORY', '1g')}",
        "--conf", "spark.sql.session.timeZone=UTC",
        "--conf", "spark.driver.extraJavaOptions=-Duser.timezone=UTC",
        "--conf", "spark.executor.extraJavaOptions=-Duser.timezone=UTC",
        "--conf", f"spark.hadoop.fs.defaultFS={env.get('HDFS_URI', 'hdfs://hdfs-namenode:8020')}",
        "--py-files", f"{JOB_DIR / 'bgl_transform.py'},{JOB_DIR / 'pipeline_io.py'}",
        str(JOB_DIR / script),
        *[str(a) for a in args],
    ]


def _run(job_id: str, cmd: list[str], log_file: Path) -> None:
    with open(log_file, "w") as out:
        proc = subprocess.run(cmd, stdout=out, stderr=subprocess.STDOUT, cwd=JOB_DIR)
    with _lock:
        _runs[job_id]["returncode"] = proc.returncode
        _runs[job_id]["status"] = "succeeded" if proc.returncode == 0 else "failed"
    log.info("job %s finished with return code %s", job_id, proc.returncode)


def start_job(name: str, args: list) -> str:
    if name not in JOBS:
        raise ValueError(f"unknown job '{name}', allowed: {sorted(JOBS)}")
    script, n_args = JOBS[name]
    if len(args) != n_args:
        raise ValueError(f"job '{name}' needs {n_args} arguments")
    args = [int(a) for a in args]  # only integers are accepted
    job_id = f"{name}-{'-'.join(map(str, args))}-{uuid.uuid4().hex[:8]}"
    log_file = LOG_DIR / f"{job_id}.log"
    with _lock:
        _runs[job_id] = {"job": name, "args": args, "status": "running", "returncode": None,
                         "log_file": str(log_file)}
    cmd = spark_submit_cmd(script, args)
    log.info("starting %s: %s", job_id, " ".join(cmd))
    threading.Thread(target=_run, args=(job_id, cmd, log_file), daemon=True).start()
    return job_id


def job_status(job_id: str) -> dict | None:
    with _lock:
        run = dict(_runs.get(job_id) or {})
    if not run:
        return None
    try:
        lines = Path(run["log_file"]).read_text(errors="replace").splitlines()
        # skip Spark's INFO noise, keep the job's own output and errors
        useful = [line for line in lines if " INFO " not in line]
        run["log_tail"] = "\n".join(useful[-40:])
    except FileNotFoundError:
        run["log_tail"] = ""
    return run


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
            return self._send(200, {"status": "ok"})
        if self.path.startswith("/jobs/"):
            run = job_status(self.path.removeprefix("/jobs/"))
            return self._send(200, run) if run else self._send(404, {"error": "unknown job id"})
        self._send(404, {"error": "not found"})

    def do_POST(self):
        if self.path != "/jobs":
            return self._send(404, {"error": "not found"})
        try:
            req = json.loads(self.rfile.read(int(self.headers.get("Content-Length", 0))) or b"{}")
            job_id = start_job(req.get("job", ""), list(req.get("args", [])))
            self._send(202, {"job_id": job_id, "status": "running"})
        except (ValueError, TypeError, json.JSONDecodeError) as e:
            self._send(400, {"error": str(e)})

    def log_message(self, fmt, *args):
        log.info("%s %s", self.address_string(), fmt % args)


if __name__ == "__main__":
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    port = int(os.environ.get("PORT", "8000"))
    log.info("spark gateway listening on :%s", port)
    ThreadingHTTPServer(("0.0.0.0", port), Handler).serve_forever()
