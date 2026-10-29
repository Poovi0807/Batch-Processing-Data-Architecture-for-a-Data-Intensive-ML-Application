"""Small helpers for months ("2005-06") and quarters ("2005Q3").

Pure Python, no Spark needed, so they are easy to unit test.
"""
import re

MONTH_PATTERN = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")
QUARTER_PATTERN = re.compile(r"^\d{4}Q[1-4]$")


def check_month(month: str) -> str:
    if not MONTH_PATTERN.match(month):
        raise ValueError(f"Month must look like 2005-06, got {month!r}")
    return month


def check_quarter(quarter: str) -> str:
    if not QUARTER_PATTERN.match(quarter):
        raise ValueError(f"Quarter must look like 2005Q3, got {quarter!r}")
    return quarter


def quarter_of(month: str) -> str:
    """'2005-08' -> '2005Q3'"""
    year, mon = check_month(month).split("-")
    return f"{year}Q{(int(mon) - 1) // 3 + 1}"


def months_of_quarter(quarter: str) -> list[str]:
    """'2005Q3' -> ['2005-07', '2005-08', '2005-09']"""
    year, q = check_quarter(quarter).split("Q")
    first = (int(q) - 1) * 3 + 1
    return [f"{year}-{m:02d}" for m in range(first, first + 3)]


def is_quarter_end(month: str) -> bool:
    """True for March, June, September and December."""
    return int(check_month(month)[5:7]) % 3 == 0
