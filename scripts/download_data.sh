#!/bin/bash
# Downloads the BGL dataset from the official Loghub record on Zenodo (57.5 MB zip)
# and splits it into monthly files in data/landing/.
# Windows users: run scripts\download_data.ps1 in PowerShell instead.
set -euo pipefail
cd "$(dirname "$0")/.."

URL="https://zenodo.org/records/8196385/files/BGL.zip?download=1"
mkdir -p data/download
curl -L --fail -o data/download/BGL.zip "$URL"

# A failed download is often an empty or tiny file: stop with a clear message
SIZE=$(wc -c < data/download/BGL.zip)
if [ "$SIZE" -lt 50000000 ]; then
  echo "Download incomplete ($SIZE bytes). Please try again." >&2
  exit 1
fi

unzip -o -q data/download/BGL.zip -d data/download
python scripts/split_by_month.py "$(find data/download -name 'BGL.log' | head -n 1)"
