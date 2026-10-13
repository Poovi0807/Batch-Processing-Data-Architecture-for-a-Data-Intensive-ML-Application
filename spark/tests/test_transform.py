"""Unit tests of the data logic (no HDFS or database needed).

    docker compose exec spark-gateway /opt/spark/bin/spark-submit /opt/tests/test_transform.py
"""
import sys
import unittest
from datetime import datetime

sys.path.insert(0, "/opt/jobs")
sys.path.insert(0, __file__.rsplit("/tests/", 1)[0] + "/jobs")   # when run outside Docker

from pyspark.sql import SparkSession            # noqa: E402
from transform import clean_lines, hourly_features   # noqa: E402

LINES = [
    # normal line: 17:42 local time = 00:42 UTC on 2005-06-04
    "- 1117845770 2005.06.03 R02-M1-N0-C:J12-U11 2005-06-03-17.42.50.675872 R02-M1-N0-C:J12-U11 RAS KERNEL INFO instruction cache parity error corrected",
    # the same line again -> duplicate, removed
    "- 1117845770 2005.06.03 R02-M1-N0-C:J12-U11 2005-06-03-17.42.50.675872 R02-M1-N0-C:J12-U11 RAS KERNEL INFO instruction cache parity error corrected",
    # contains an IP address -> masked
    "- 1117845800 2005.06.03 R02-M1-N0-I:J18-U11 2005-06-03-17.43.20.100000 R02-M1-N0-I:J18-U11 RAS APP FATAL ciod: failed to read message prefix on control stream (CioStream socket to 172.16.96.116:33569",
    # alert one hour later on the same midplane
    "KERNDTLB 1117849500 2005.06.03 R02-M1-N1-C:J02-U01 2005-06-03-18.45.00.000000 R02-M1-N1-C:J02-U01 RAS KERNEL FATAL data TLB error interrupt",
    # node without a midplane
    "- 1117849600 2005.06.03 NULL 2005-06-03-18.46.40.000000 NULL RAS MMCS ERROR idoproxydb hit ASSERT condition",
    # broken line
    "this line is broken",
]


class TransformTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (SparkSession.builder.master("local[1]").config("spark.sql.session.timeZone", "UTC")
                     .config("spark.ui.enabled", "false").getOrCreate())
        raw = cls.spark.createDataFrame([(line,) for line in LINES], ["value"])
        cls.clean, cls.report = clean_lines(raw)
        cls.rows = cls.clean.orderBy("event_time").collect()

    def test_counts(self):
        self.assertEqual(self.report["lines_in"], 6)
        self.assertEqual(self.report["duplicates"], 1)
        self.assertEqual(self.report["broken"], 1)
        self.assertEqual(self.report["lines_out"], 4)

    def test_columns(self):
        first = self.rows[0]
        self.assertEqual(first.event_time, datetime(2005, 6, 4, 0, 42, 50))
        self.assertEqual(first.midplane, "R02-M1")
        self.assertEqual(first.level, "INFO")
        self.assertFalse(first.is_alert)
        self.assertTrue(self.rows[2].is_alert)
        self.assertEqual(self.rows[2].alert_type, "KERNDTLB")

    def test_ip_is_masked(self):
        self.assertNotIn("172.16.96.116", self.rows[1].message)
        self.assertIn("<IP>", self.rows[1].message)

    def test_features_and_label(self):
        rows = hourly_features(self.clean, "2005Q2").orderBy("hour_start").collect()
        self.assertEqual(len(rows), 2)                 # midplane R02-M1, 00:00 and 01:00 UTC
        self.assertEqual(rows[0].event_count, 2)
        self.assertTrue(rows[0].alert_next_hour)       # alert at 01:45
        self.assertFalse(rows[1].alert_next_hour)


if __name__ == "__main__":
    unittest.main(verbosity=2)
