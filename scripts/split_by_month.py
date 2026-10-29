"""Split the single BGL.log file into one file per month, like monthly log shipments.

    python scripts/split_by_month.py path/to/BGL.log

creates data/landing/BGL_2005-06.log ... data/landing/BGL_2006-01.log.
Only the Python standard library is needed. A line whose date cannot be read stays
in the file of the line before it, so nothing is lost; the cleaning job drops it later.
"""
import re
import sys
from pathlib import Path

DATE = re.compile(rb"^\S+ \d+ (\d{4})\.(\d{2})\.\d{2} ")


def split(source: Path, target_dir: Path) -> dict:
    target_dir.mkdir(parents=True, exist_ok=True)
    handles, counts, month = {}, {}, None
    try:
        with source.open("rb") as lines:
            for line in lines:
                match = DATE.match(line)
                if match:
                    month = f"{match[1].decode()}-{match[2].decode()}"
                if month is None:          # broken lines before the first good line
                    continue
                if month not in handles:
                    handles[month] = (target_dir / f"BGL_{month}.log").open("wb")
                    counts[month] = 0
                handles[month].write(line)
                counts[month] += 1
    finally:
        for handle in handles.values():
            handle.close()
    return counts


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit("usage: python scripts/split_by_month.py path/to/BGL.log")
    landing = Path(__file__).resolve().parent.parent / "data" / "landing"
    result = split(Path(sys.argv[1]), landing)
    for month, count in sorted(result.items()):
        print(f"BGL_{month}.log  {count:>10,} lines")
    print(f"total           {sum(result.values()):>10,} lines -> {landing}")
