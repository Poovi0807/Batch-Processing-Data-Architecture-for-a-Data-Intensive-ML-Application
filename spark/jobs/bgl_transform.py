"""Pure Spark transformations for the BGL pipeline.

Kept free of any I/O so they can be unit-tested with a local SparkSession
(see spark/tests/test_transform.py).
"""
from pyspark.sql import DataFrame, functions as F

# fields: alert label, unix time, date, node, timestamp, node, event type, component, level, message
LINE_RE = (
    r"^(\S+) (\d+) (\d{4}\.\d{2}\.\d{2}) (\S+) "
    r"(\d{4}-\d{2}-\d{2}-\d{2}\.\d{2}\.\d{2}\.\d+) (\S+) (\S+) (\S+) (\S+)(?: (.*))?$"
)

# personal or network data that must not leave the raw zone (GDPR, data minimisation)
IPV4_RE = r"\b(?:(?:25[0-5]|2[0-4]\d|1?\d?\d)\.){3}(?:25[0-5]|2[0-4]\d|1?\d?\d)\b"
EMAIL_RE = r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}"
USER_RE = r"(?i)\b(user|uid|login)[=: ]+\S+"

LEVELS = ["INFO", "WARNING", "SEVERE", "ERROR", "FATAL", "FAILURE"]
COMPONENTS = ["KERNEL", "APP", "MMCS", "DISCOVERY", "MONITOR", "HARDWARE"]


def parse_and_clean(raw: DataFrame):
    """raw: DataFrame with one column `value` (one log line per row).

    Returns (clean_df, stats) where stats counts input, duplicate and malformed lines.
    """
    lines = raw.withColumn("value", F.regexp_replace("value", r"\r$", ""))
    rows_in = lines.count()

    dedup = lines.dropDuplicates(["value"])
    rows_dedup = dedup.count()

    ok = F.col("value").rlike(LINE_RE)
    malformed = dedup.filter(~ok).count()
    valid = dedup.filter(ok)

    g = lambda i: F.regexp_extract("value", LINE_RE, i)  # noqa: E731
    parsed = valid.select(
        g(1).alias("alert_label"),
        g(2).cast("long").alias("epoch_s"),
        g(4).alias("node"),
        g(7).alias("event_type"),
        g(8).alias("component"),
        g(9).alias("level"),
        g(10).alias("message"),
    )

    masked = F.regexp_replace(F.regexp_replace(F.regexp_replace(
        F.col("message"), IPV4_RE, "<IP>"), EMAIL_RE, "<EMAIL>"), USER_RE, "$1=<USER>")
    template = F.trim(F.regexp_replace(F.regexp_replace(F.regexp_replace(
        F.col("message_masked"), r"0x[0-9a-fA-F]+", "<HEX>"), r"\d+", "<NUM>"), r"\s+", " "))

    clean = (
        parsed
        .withColumn("event_time", F.timestamp_seconds("epoch_s"))
        .withColumn("is_alert", F.col("alert_label") != "-")
        .withColumn("alert_category", F.when(F.col("alert_label") != "-", F.col("alert_label")))
        .withColumn("rack", F.nullif(F.regexp_extract("node", r"^(R\d{2})", 1), F.lit("")))
        .withColumn("midplane", F.nullif(F.regexp_extract("node", r"^(R\d{2}-M\d)", 1), F.lit("")))
        .withColumn("message_masked", masked)
        .withColumn("template", template)
        .drop("message", "alert_label")
        .select("event_time", "epoch_s", "node", "rack", "midplane", "event_type", "component",
                "level", "is_alert", "alert_category", "message_masked", "template")
    )
    stats = {
        "rows_in": rows_in,
        "duplicates_removed": rows_in - rows_dedup,
        "malformed": malformed,
        "rows_out": rows_dedup - malformed,
        "malformed_ratio": (malformed / rows_in) if rows_in else 0.0,
    }
    return clean, stats


def build_hourly_features(events: DataFrame, quarter: str) -> DataFrame:
    """One row per midplane and hour, plus the ML label `alert_next_hour`."""
    ev = events.filter(F.col("midplane").isNotNull()).withColumn(
        "hour_start", F.date_trunc("hour", "event_time"))

    count_if = lambda cond: F.sum(F.when(cond, 1).otherwise(0)).cast("int")  # noqa: E731
    aggs = [F.count(F.lit(1)).cast("int").alias("event_count")]
    aggs += [count_if(F.col("level") == lv).alias(f"cnt_{lv.lower()}") for lv in LEVELS]
    aggs += [count_if(F.col("component") == c).alias(f"cnt_{c.lower()}") for c in COMPONENTS]
    aggs += [
        count_if(~F.col("component").isin(COMPONENTS)).alias("cnt_other_component"),
        F.countDistinct("template").cast("int").alias("distinct_templates"),
        F.countDistinct("node").cast("int").alias("distinct_nodes"),
        count_if(F.col("is_alert")).alias("alert_count"),
    ]
    hourly = ev.groupBy("midplane", "hour_start").agg(*aggs)

    # label: did the same midplane raise at least one alert in the following hour?
    next_alerts = (hourly.filter(F.col("alert_count") > 0)
                   .select("midplane", (F.col("hour_start") - F.expr("INTERVAL 1 HOUR")).alias("hour_start"))
                   .withColumn("alert_next_hour", F.lit(True)))
    return (hourly.join(next_alerts, ["midplane", "hour_start"], "left")
            .withColumn("alert_next_hour", F.coalesce("alert_next_hour", F.lit(False)))
            .withColumn("quarter", F.lit(quarter))
            .select("quarter", "midplane", "hour_start", "event_count",
                    *[f"cnt_{lv.lower()}" for lv in LEVELS],
                    *[f"cnt_{c.lower()}" for c in COMPONENTS],
                    "cnt_other_component", "distinct_templates", "distinct_nodes",
                    "alert_count", "alert_next_hour"))
