import os
import shutil
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path[:0] = [str(ROOT), str(ROOT / "ingest")]

SAMPLE = ROOT / "tests" / "sample" / "BGL_2k.log"


@pytest.fixture(scope="session")
def spark():
    """A small local Spark session. Needs Java 17 or newer; skipped otherwise."""
    if shutil.which("java") is None:
        pytest.skip("Java is not installed, Spark tests skipped")
    # Spark's Python workers must use the same Python version as the tests
    os.environ["PYSPARK_PYTHON"] = sys.executable
    from pyspark.sql import SparkSession
    session = (SparkSession.builder.master("local[2]").appName("tests")
               .config("spark.sql.session.timeZone", "UTC")
               .config("spark.sql.shuffle.partitions", "4")
               .config("spark.ui.enabled", "false")
               .getOrCreate())
    yield session
    session.stop()
