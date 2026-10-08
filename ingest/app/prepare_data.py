"""Download the BGL dataset once and split it into monthly landing files.

This simulates how log files arrive in production: one file per month in a
landing folder. Run it once before starting the pipeline:

    docker compose run --rm ingest python prepare_data.py

Source:
  * Kaggle "LogHub - BGL Log Data" if KAGGLE_USERNAME and KAGGLE_KEY are set,
  * otherwise the original Loghub archive on Zenodo (identical data),
  * or a BGL.log (or .zip) that you copied into data/download yourself.
"""
import os
import sys
import zipfile
from pathlib import Path

import requests

from bgl_format import landing_name, month_of_line

KAGGLE_URL = "https://www.kaggle.com/api/v1/datasets/download/omduggineni/loghub-bgl-log-data"
ZENODO_URL = "https://zenodo.org/records/8196385/files/BGL.zip?download=1"

DOWNLOAD_DIR = Path(os.environ.get("DOWNLOAD_DIR", "/data/download"))
LANDING_DIR = Path(os.environ.get("LANDING_DIR", "/data/landing"))


def download(url: str, target: Path, auth=None) -> None:
    print(f"Downloading {url} ...", flush=True)
    with requests.get(url, stream=True, auth=auth, timeout=60) as r:
        r.raise_for_status()
        tmp = target.with_suffix(".part")
        with open(tmp, "wb") as f:
            for chunk in r.iter_content(chunk_size=1 << 20):
                f.write(chunk)
        tmp.rename(target)
    print(f"Saved {target} ({target.stat().st_size / 1e6:.1f} MB)")


def find_log_file() -> Path:
    """Return the path of BGL.log, downloading and unzipping it if needed."""
    logs = sorted(DOWNLOAD_DIR.rglob("*.log"))
    if logs:
        return max(logs, key=lambda p: p.stat().st_size)

    zips = sorted(DOWNLOAD_DIR.glob("*.zip"))
    if not zips:
        user, key = os.environ.get("KAGGLE_USERNAME"), os.environ.get("KAGGLE_KEY")
        target = DOWNLOAD_DIR / "bgl.zip"
        if user and key:
            download(KAGGLE_URL, target, auth=(user, key))
        else:
            print("No Kaggle credentials set, using the original Loghub archive on Zenodo.")
            download(ZENODO_URL, target)
        zips = [target]

    with zipfile.ZipFile(zips[0]) as z:
        members = [m for m in z.namelist() if m.endswith(".log")]
        if not members:
            sys.exit(f"No .log file inside {zips[0]}")
        member = max(members, key=lambda m: z.getinfo(m).file_size)
        print(f"Extracting {member} ...")
        z.extract(member, DOWNLOAD_DIR)
        return DOWNLOAD_DIR / member


def split_by_month(log_file: Path) -> dict:
    """Write one landing file per month. Lines without a readable date stay
    with the previous line's month, so malformed lines are not lost and the
    pipeline's quality checks can see them."""
    LANDING_DIR.mkdir(parents=True, exist_ok=True)
    handles, counts, current = {}, {}, None
    try:
        with open(log_file, "r", encoding="utf-8", errors="replace") as src:
            for line in src:
                ym = month_of_line(line) or current
                if ym is None:
                    continue
                current = ym
                if ym not in handles:
                    handles[ym] = open(LANDING_DIR / (landing_name(*ym) + ".part"), "w", encoding="utf-8")
                    counts[ym] = 0
                handles[ym].write(line if line.endswith("\n") else line + "\n")
                counts[ym] += 1
    finally:
        for h in handles.values():
            h.close()
    for ym in handles:
        part = LANDING_DIR / (landing_name(*ym) + ".part")
        part.replace(LANDING_DIR / landing_name(*ym))
    return counts


def main() -> None:
    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
    log_file = find_log_file()
    print(f"Splitting {log_file} into monthly files ...", flush=True)
    counts = split_by_month(log_file)
    total = sum(counts.values())
    for (y, m), n in sorted(counts.items()):
        print(f"  {landing_name(y, m)}  {n:>10,} lines")
    print(f"Total: {total:,} lines in {len(counts)} monthly files in {LANDING_DIR}")


if __name__ == "__main__":
    main()
