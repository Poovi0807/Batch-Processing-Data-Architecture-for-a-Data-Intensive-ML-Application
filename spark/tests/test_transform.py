"""Unit tests for the Spark transformations (no HDFS or database needed).

Run inside the Spark container:
    docker compose run --rm --no-deps spark-gateway /opt/spark/bin/spark-submit /opt/pipeline/tests/test_transform.py
"""
import os
import sys
import unittest
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "jobs"))
sys.path.insert(0, "/opt/pipeline/jobs")

from pyspark.sql import SparkSession  # noqa: E402

from bgl_transform import build_hourly_features, parse_and_clean  # noqa: E402

LINES = [
    # normal line (00:42 UTC on 2005-06-04)
    "- 1117845770 2005.06.03 R02-M1-N0-C:J12-U11 2005-06-03-17.42.50.675872 R02-M1-N0-C:J12-U11 RAS KERNEL INFO instruction cache parity error corrected",
    # exact duplicate of the line above -> removed
    "- 1117845770 2005.06.03 R02-M1-N0-C:J12-U11 2005-06-03-17.42.50.675872 R02-M1-N0-C:J12-U11 RAS KERNEL INFO instruction cache parity error corrected",
    # contains an IP address -> masked
    "- 1117845800 2005.06.03 R02-M1-N0-I:J18-U11 2005-06-03-17.43.20.100000 R02-M1-N0-I:J18-U11 RAS APP FATAL ciod: failed to read message prefix on control stream (CioStream socket to 172.16.96.116:33569",
    # alert in the next hour on the same midplane
    "KERNDTLB 1117849500 2005.06.03 R02-M1-N1-C:J02-U01 2005-06-03-18.45.00.000000 R02-M1-N1-C:J02-U01 RAS KERNEL FATAL data TLB error interrupt",
    # node without a midplane
    "- 1117849600 2005.06.03 NULL 2005-06-03-18.46.40.000000 NULL RAS MMCS ERROR idoproxydb hit ASSERT condition",
    # malformed line
    "this line is broken",
]


class TransformTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (SparkSession.builder.master("local[2]").appName("tests")
                     .config("spark.sql.session.timeZone", "UTC")
                     .config("spark.ui.enabled", "false").getOrCreate())
        raw = cls.spark.createDataFrame([(line,) for line in LINES], ["value"])
        cls.clean, cls.stats = parse_and_clean(raw)
        cls.rows = cls.clean.orderBy("epoch_s").collect()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_quality_stats(self):
        self.assertEqual(self.stats["rows_in"], 6)
        self.assertEqual(self.stats["duplicates_removed"], 1)
        self.assertEqual(self.stats["malformed"], 1)
        self.assertEqual(self.stats["rows_out"], 4)

    def test_fields(self):
        first = self.rows[0]
        self.assertEqual(first.event_time, datetime(2005, 6, 4, 0, 42, 50))
        self.assertEqual(first.midplane, "R02-M1")
        self.assertEqual(first.rack, "R02")
        self.assertEqual(first.level, "INFO")
        self.assertFalse(first.is_alert)
        alert = self.rows[2]
        self.assertTrue(alert.is_alert)
        self.assertEqual(alert.alert_category, "KERNDTLB")
        self.assertIsNone(self.rows[3].midplane)

    def test_ip_masked(self):
        msg = self.rows[1].message_masked
        self.assertNotIn("172.16.96.116", msg)
        self.assertIn("<IP>", msg)
        self.assertNotIn("33569", self.rows[1].template)

    def test_features_and_label(self):
        feats = build_hourly_features(self.clean, "2005Q2").orderBy("hour_start").collect()
        self.assertEqual(len(feats), 2)  # midplane R02-M1, hours 00:00 and 01:00 UTC
        h0, h1 = feats
        self.assertEqual(h0.event_count, 2)
        self.assertEqual(h0.cnt_fatal, 1)
        self.assertTrue(h0.alert_next_hour)   # alert at 01:45
        self.assertEqual(h1.alert_count, 1)
        self.assertFalse(h1.alert_next_hour)
        self.assertEqual(h0.quarter, "2005Q2")


if __name__ == "__main__":
    unittest.main(verbosity=2)
