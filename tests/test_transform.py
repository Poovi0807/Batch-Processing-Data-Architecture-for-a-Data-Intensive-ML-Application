"""Tests for the Spark transformations, run on the real BGL sample."""
from datetime import datetime

from pyspark.sql import Row
from pyspark.sql import functions as F

from conftest import SAMPLE
from jobs.transform import clean, hourly_features, parse_lines


def sample_lines(spark):
    return spark.read.text(str(SAMPLE))


def test_every_sample_line_is_parsed(spark):
    parsed = parse_lines(sample_lines(spark))
    assert parsed.count() == 2000
    assert parsed.filter(F.col("unix_ts") == "").count() == 0


def test_clean_keeps_only_the_month(spark):
    june = clean(parse_lines(sample_lines(spark)), "2005-06")
    assert june.count() == 498                        # June lines in the sample
    assert june.filter(F.date_format("event_time", "yyyy-MM") != "2005-06").count() == 0


def test_clean_drops_broken_and_duplicate_lines(spark):
    first = SAMPLE.read_text().splitlines()[0]        # a real June line
    lines = [first, first,                            # exact duplicate
             "garbage that is not a log line",        # wrong format
             first.replace("2005-06-03-15.42.50", "2005-06-31-15.42.50")]  # impossible date
    raw = spark.createDataFrame([(line,) for line in lines], ["value"])
    assert clean(parse_lines(raw), "2005-06").count() == 1


def test_personal_data_is_masked(spark):
    events = clean(parse_lines(sample_lines(spark)), "2005-11")
    messages = [r.message for r in events.select("message").collect()]
    assert not any("172.16.96.116" in m for m in messages)
    assert any("<IP>" in m for m in messages)


def test_location_and_templates(spark):
    events = clean(parse_lines(sample_lines(spark)), "2005-06")
    first = events.orderBy("event_time").first()
    assert (first.rack, first.midplane) == ("R02", "R02-M1")
    assert "<NUM>" in first.template or first.template == first.message
    assert events.filter(F.col("midplane") == "").count() == 0


def test_features_add_up_to_the_events(spark):
    events = clean(parse_lines(sample_lines(spark)), "2005-07")
    features = hourly_features(events, "2005Q3")
    known = events.filter(F.col("midplane") != "UNKNOWN")
    assert features.agg(F.sum("events_total")).first()[0] == known.count()
    assert features.agg(F.sum("alerts")).first()[0] == known.filter("is_alert").count()
    assert features.groupBy("midplane", "hour_start").count().filter("count > 1").count() == 0


def test_label_alert_next_hour(spark):
    def event(hour, alert):
        return Row(event_time=datetime(2005, 7, 1, hour, 5), midplane="R01-M0", level="INFO",
                   component="KERNEL", template="t", is_alert=alert)
    events = spark.createDataFrame([event(10, False), event(11, True), event(13, False)])
    labels = {r.hour_start.hour: r.alert_next_hour
              for r in hourly_features(events, "2005Q3").collect()}
    assert labels == {10: True, 11: False, 13: False}
