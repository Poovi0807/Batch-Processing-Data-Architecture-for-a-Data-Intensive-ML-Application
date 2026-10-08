"""Shared knowledge about the BGL log format (used by validation and data preparation)."""
import re

# Example line:
# - 1117838570 2005.06.03 R02-M1-N0-C:J12-U11 2005-06-03-15.42.50.675872 R02-M1-N0-C:J12-U11 RAS KERNEL INFO instruction cache parity error corrected
# fields: alert label, unix time, date, node, timestamp, node, event type, component, level, message
LINE_RE = re.compile(
    r"^(\S+) (\d+) (\d{4})\.(\d{2})\.(\d{2}) (\S+) "
    r"(\d{4}-\d{2}-\d{2}-\d{2}\.\d{2}\.\d{2}\.\d+) (\S+) (\S+) (\S+) (\S+)(?: (.*))?$"
)

# landing files are named bgl_YYYY-MM.log
FILE_RE = re.compile(r"^bgl_(\d{4})-(\d{2})\.log$")


def landing_name(year: int, month: int) -> str:
    return f"bgl_{year:04d}-{month:02d}.log"


def month_of_line(line: str):
    """Return (year, month) from the date field of a line, or None if it cannot be read."""
    parts = line.split(" ", 3)
    if len(parts) < 3:
        return None
    m = re.fullmatch(r"(\d{4})\.(\d{2})\.\d{2}", parts[2])
    if not m:
        return None
    return int(m.group(1)), int(m.group(2))
