"""Create a .env file with random passwords and keys (Python standard library only).

    python scripts/generate_env.py
"""
import base64
import os
import secrets
from pathlib import Path

root = Path(__file__).resolve().parent.parent
target = root / ".env"
if target.exists():
    raise SystemExit(".env already exists - delete it first if you want new secrets")

values = {
    "SERVING_ADMIN_PASSWORD": secrets.token_urlsafe(18),
    "SERVING_ETL_PASSWORD": secrets.token_urlsafe(18),
    "SERVING_ML_PASSWORD": secrets.token_urlsafe(18),
    "AIRFLOW_DB_PASSWORD": secrets.token_urlsafe(18),
    "AIRFLOW_ADMIN_PASSWORD": secrets.token_urlsafe(12),
    "AIRFLOW_FERNET_KEY": base64.urlsafe_b64encode(os.urandom(32)).decode(),
    "AIRFLOW_JWT_SECRET": secrets.token_urlsafe(32),
}

lines = []
for line in (root / ".env.example").read_text().splitlines():
    key = line.split("=", 1)[0]
    lines.append(f"{key}={values[key]}" if key in values else line)
target.write_text("\n".join(lines) + "\n")
print(f"wrote {target}")
print(f"Airflow login: admin / {values['AIRFLOW_ADMIN_PASSWORD']}")
