"""Load-test data: build a big BGL-like file by replaying the real 2,000-line sample.

    python scripts/make_replay_data.py tests/sample/BGL_2k.log 2400 replay/BGL.log

Every copy keeps the real line format, levels and alert labels, but gets a new
microsecond in its timestamps so lines stay unique. Use it ONLY to test speed and
volume when the real 4.7 million-line file is not at hand; results from it say
nothing about the real BGL system.
"""
import re
import sys
from pathlib import Path

FULL_TS = re.compile(rb"(\d{4}-\d{2}-\d{2}-\d{2}\.\d{2}\.\d{2}\.)(\d{6})")


def replay(sample: Path, copies: int, target: Path) -> int:
    lines = [line + b"\n" for line in sample.read_bytes().splitlines()]  # last line may lack \n
    target.parent.mkdir(parents=True, exist_ok=True)
    written = 0
    with target.open("wb") as out:
        for line in lines:                      # keep the month order of the sample
            for copy in range(copies):
                micro = lambda m: m[1] + f"{(int(m[2]) + copy) % 1_000_000:06d}".encode()
                out.write(FULL_TS.sub(micro, line, count=1))
                written += 1
    return written


if __name__ == "__main__":
    sample_file, n_copies, output = Path(sys.argv[1]), int(sys.argv[2]), Path(sys.argv[3])
    print(f"{replay(sample_file, n_copies, output):,} lines written to {output}")
