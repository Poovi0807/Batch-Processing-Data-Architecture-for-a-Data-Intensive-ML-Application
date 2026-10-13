"""STEP 0 (run once): download the BGL log and split it into one file per month.

    docker compose run --rm ingest python prepare_data.py

Result: data/landing/bgl_2005-06.log ... data/landing/bgl_2006-01.log
If the download keeps failing, download BGL.zip in your browser from the URL
below and save it in the folder data/download - this script then uses it.
"""
import os
import time
import zipfile
from pathlib import Path

import requests

URL = "https://zenodo.org/records/8196385/files/BGL.zip?download=1"  # original Loghub archive
DOWNLOAD = Path(os.environ.get("DOWNLOAD_DIR", "/data/download"))
LANDING = Path(os.environ.get("LANDING_DIR", "/data/landing"))


def get_zip() -> Path:
    """Return the BGL zip file. Download it if it is not there yet (5 attempts)."""
    zips = list(DOWNLOAD.glob("*.zip"))
    if zips:
        print(f"Using {zips[0]}")
        return zips[0]

    target = DOWNLOAD / "BGL.zip"
    for attempt in range(1, 6):
        try:
            print(f"Downloading {URL} (attempt {attempt} of 5) ...", flush=True)
            with requests.get(URL, stream=True, timeout=120) as r:
                r.raise_for_status()
                with open(DOWNLOAD / "BGL.part", "wb") as f:
                    for chunk in r.iter_content(chunk_size=1 << 20):
                        f.write(chunk)
            (DOWNLOAD / "BGL.part").rename(target)
            return target
        except requests.RequestException as error:
            print(f"Download failed: {error}")
            time.sleep(10 * attempt)
    raise SystemExit("Download failed 5 times. Save BGL.zip from the URL above in data/download "
                     "and run this script again.")


def split_by_month(zip_path: Path) -> dict:
    """Read the log inside the zip and write one landing file per month.
    The month comes from the date field of each line (e.g. 2005.06.03 -> 2005-06)."""
    LANDING.mkdir(parents=True, exist_ok=True)
    files, counts, month = {}, {}, None
    with zipfile.ZipFile(zip_path) as z:
        log_name = next(n for n in z.namelist() if n.endswith(".log"))
        with z.open(log_name) as log:
            for raw in log:
                line = raw.decode("utf-8", errors="replace").rstrip("\r\n")
                fields = line.split(" ", 3)
                date = fields[2] if len(fields) > 2 else ""
                if len(date) == 10 and date[4] == "." and date[7] == ".":
                    month = date[:7].replace(".", "-")      # 2005.06.03 -> 2005-06
                if month is None:
                    continue                                 # no month known yet
                if month not in files:
                    files[month] = open(LANDING / f"bgl_{month}.log", "w", encoding="utf-8")
                    counts[month] = 0
                files[month].write(line + "\n")
                counts[month] += 1
    for f in files.values():
        f.close()
    return counts


if __name__ == "__main__":
    DOWNLOAD.mkdir(parents=True, exist_ok=True)
    counts = split_by_month(get_zip())
    for month, n in sorted(counts.items()):
        print(f"  bgl_{month}.log  {n:>10,} lines")
    print(f"Total: {sum(counts.values()):,} lines in {len(counts)} files in {LANDING}")
