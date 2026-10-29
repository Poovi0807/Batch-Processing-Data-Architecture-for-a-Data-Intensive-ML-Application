#!/bin/bash
# Downloads the BGL dataset from Kaggle and splits it into monthly files.
# Needs a Kaggle account: pip install kaggle, then put your API token in ~/.kaggle/kaggle.json
# Without Kaggle: download BGL.zip by hand from https://zenodo.org/records/8196385,
# unzip it and run:  python scripts/split_by_month.py BGL.log
set -euo pipefail
cd "$(dirname "$0")/.."

mkdir -p data/download
kaggle datasets download -d omduggineni/loghub-bgl-log-data -p data/download --unzip
LOG_FILE=$(find data/download -name "BGL*.log" -size +100M | head -n 1)
python scripts/split_by_month.py "$LOG_FILE"
