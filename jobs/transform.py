"""Spark transformations for the BGL logs.

Only DataFrame functions are used (no Python UDFs). The same code therefore runs
on a local SparkSession (unit tests) and through Spark Connect (in the pipeline).
"""
from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

# One BGL line looks like this:
# - 1117838570 2005.06.03 R02-M1-N0-C:J12-U11 2005-06-03-15.42.50.675872 R02-M1-N0-C:J12-U11 RAS KERNEL INFO instruction cache ...
LINE_PATTERN = (
    r"^(\S+) (\d+) (\d{4}\.\d{2}\.\d{2}) (\S+) "
    r"(\d{4}-\d{2}-\d{2}-\d{2}\.\d{2}\.\d{2}\.\d{6}) (\S+) (\S+) (\S+) (\S+) ?(.*)$"
)
FIELDS = [
    "alert_label", "unix_ts", "log_date", "node", "event_ts_raw",
    "node_repeat", "event_type", "component", "level", "message",
]
TIMESTAMP_FORMAT = "yyyy-MM-dd-HH.mm.ss.SSSSSS"

PROCESSED_COLUMNS = [
    "event_time", "node", "rack", "midplane", "event_type", "component",
    "level", "is_alert", "alert_category", "message", "template",
]


def parse_lines(raw: DataFrame) -> DataFrame:
    """Split each raw text line (column 'value') into named columns.

    Lines that do not match the pattern get empty strings; clean() drops them.
    """
    columns = [
        F.regexp_extract("value", LINE_PATTERN, i + 1).alias(name)
        for i, name in enumerate(FIELDS)
    ]
    return raw.select(F.col("value").alias("raw_line"), *columns)


def mask_personal_data(message: Column) -> Column:
    """Replace e-mail and IP addresses before data leaves the raw zone (GDPR)."""
    message = F.regexp_replace(message, r"[\w.+-]+@[\w-]+\.[\w.]+", "<EMAIL>")
    message = F.regexp_replace(message, r"\b\d{1,3}(\.\d{1,3}){3}\b", "<IP>")
    return message


def to_template(message: Column) -> Column:
    """Turn a message into a template by masking hex values and numbers.

    'data TLB error interrupt at 0x1f3a' -> 'data TLB error interrupt at <HEX>'
    """
    message = F.regexp_replace(message, r"0x[0-9a-fA-F]+", "<HEX>")
    return F.regexp_replace(message, r"\d+", "<NUM>")


def location_part(node: Column, pattern: str) -> Column:
    """Extract rack ('R02') or midplane ('R02-M1') from a node ID, else 'UNKNOWN'."""
    part = F.regexp_extract(node, pattern, 1)
    return F.when(part == "", F.lit("UNKNOWN")).otherwise(part)


def clean(parsed: DataFrame, month: str) -> DataFrame:
    """Drop broken and duplicate lines, add useful columns, mask personal data."""
    # Wall-clock time without time zone, so no machine's local time zone can shift it.
    event_time = F.try_to_timestamp(F.col("event_ts_raw"), F.lit(TIMESTAMP_FORMAT)).cast("timestamp_ntz")
    return (
        parsed
        .filter(F.col("unix_ts") != "")                     # line did not match the pattern
        .withColumn("event_time", event_time)
        .filter(F.col("event_time").isNotNull())            # impossible timestamp
        .filter(F.date_format("event_time", "yyyy-MM") == month)  # line belongs to another month
        .dropDuplicates(["raw_line"])                       # exact duplicate lines
        .withColumn("is_alert", F.col("alert_label") != "-")
        .withColumn("alert_category",
                    F.when(F.col("alert_label") == "-", F.lit(None)).otherwise(F.col("alert_label")))
        .withColumn("level", F.upper("level"))
        .withColumn("rack", location_part(F.col("node"), r"^(R\d\d)"))
        .withColumn("midplane", location_part(F.col("node"), r"^(R\d\d-M\d)"))
        .withColumn("message", mask_personal_data(F.col("message")))
        .withColumn("template", to_template(F.col("message")))
        .select(*PROCESSED_COLUMNS)
    )


def count_level(*levels: str) -> Column:
    return F.sum(F.when(F.col("level").isin(*levels), 1).otherwise(0)).cast("int")


def hourly_features(events: DataFrame, quarter: str) -> DataFrame:
    """One row per midplane and hour, plus the label 'alert in the next hour'."""
    hourly = (
        events
        .filter(F.col("midplane") != "UNKNOWN")
        .withColumn("hour_start", F.date_trunc("hour", "event_time"))
        .groupBy("midplane", "hour_start")
        .agg(
            F.count("*").cast("int").alias("events_total"),
            count_level("INFO").alias("events_info"),
            count_level("WARNING").alias("events_warning"),
            count_level("ERROR", "SEVERE").alias("events_error"),
            count_level("FATAL", "FAILURE").alias("events_fatal"),
            F.sum(F.when(F.col("component") == "KERNEL", 1).otherwise(0)).cast("int").alias("kernel_events"),
            F.sum(F.when(F.col("component") == "APP", 1).otherwise(0)).cast("int").alias("app_events"),
            F.count_distinct("template").cast("int").alias("distinct_templates"),
            F.sum(F.col("is_alert").cast("int")).cast("int").alias("alerts"),
        )
    )
    # Shift each hour back by one: the alerts of hour h+1 become the label of hour h.
    next_hour = hourly.select(
        "midplane",
        (F.col("hour_start") - F.expr("INTERVAL 1 HOUR")).alias("hour_start"),
        (F.col("alerts") > 0).alias("alert_next_hour"),
    )
    return (
        hourly.join(next_hour, ["midplane", "hour_start"], "left")
        .withColumn("alert_next_hour", F.coalesce("alert_next_hour", F.lit(False)))
        .withColumn("quarter", F.lit(quarter))
        .select(
            "quarter", "midplane", "hour_start", "events_total", "events_info",
            "events_warning", "events_error", "events_fatal", "kernel_events",
            "app_events", "distinct_templates", "alerts", "alert_next_hour",
        )
    )
