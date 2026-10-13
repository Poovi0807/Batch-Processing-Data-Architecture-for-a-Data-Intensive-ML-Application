"""The data logic of the pipeline, used by both Spark jobs (and by the unit tests).

clean_lines()      raw log lines   -> clean table, one row per log line   (monthly job)
hourly_features()  clean table     -> one row per midplane and hour       (quarterly job)
"""
from pyspark.sql import functions as F

# personal or network data that must not leave the raw zone (GDPR)
IP_ADDRESS = r"\b(?:\d{1,3}\.){3}\d{1,3}\b"
EMAIL = r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}"
USER_NAME = r"(?i)\b(user|uid|login)[=: ]+\S+"

LEVELS = ["INFO", "WARNING", "SEVERE", "ERROR", "FATAL", "FAILURE"]
COMPONENTS = ["KERNEL", "APP", "MMCS", "DISCOVERY", "MONITOR", "HARDWARE"]


def clean_lines(raw):
    """raw: DataFrame with one column "value" = one log line.
    Returns (clean DataFrame, report with counts)."""
    lines_in = raw.count()
    lines = raw.dropDuplicates(["value"])                 # 1. remove identical lines

    # 2. split each line into its fields: label, unix time, date, node, time, node,
    #    type, component, level, message  (at most 10 pieces)
    f = F.split("value", " ", 10)
    good = (F.size(f) >= 9) & f[1].rlike(r"^\d+$") & f[2].rlike(r"^\d{4}\.\d{2}\.\d{2}$")
    broken = lines.filter(~good).count()

    message = F.coalesce(f[9], F.lit(""))
    masked = F.regexp_replace(F.regexp_replace(F.regexp_replace(
        message, IP_ADDRESS, "<IP>"), EMAIL, "<EMAIL>"), USER_NAME, "$1=<USER>")

    clean = lines.filter(good).select(
        F.timestamp_seconds(f[1].cast("long")).alias("event_time"),         # 3. time in UTC
        f[3].alias("node"),
        F.regexp_extract(f[3], r"^(R\d{2})", 1).alias("rack"),               # R02-M1-N0-... -> R02
        F.regexp_extract(f[3], r"^(R\d{2}-M\d)", 1).alias("midplane"),       # -> R02-M1
        f[7].alias("component"),
        f[8].alias("level"),
        (f[0] != "-").alias("is_alert"),                                     # "-" means no alert
        F.when(f[0] != "-", f[0]).alias("alert_type"),
        masked.alias("message"),                                             # 4. masked message
    ).withColumn(                                                            # 5. message template
        "template", F.regexp_replace(F.regexp_replace("message", r"0x[0-9a-fA-F]+", "<HEX>"), r"\d+", "<NUM>"))

    unique = lines.count()
    report = {"lines_in": lines_in, "duplicates": lines_in - unique, "broken": broken,
              "lines_out": unique - broken, "broken_ratio": broken / lines_in if lines_in else 0.0}
    return clean, report


def hourly_features(events, quarter: str):
    """One row per midplane and hour, plus the ML label alert_next_hour."""
    events = events.filter(F.col("midplane") != "").withColumn("hour_start", F.date_trunc("hour", "event_time"))

    def count_if(condition):
        return F.sum(F.when(condition, 1).otherwise(0)).cast("int")

    hourly = events.groupBy("midplane", "hour_start").agg(
        F.count("*").cast("int").alias("event_count"),
        *[count_if(F.col("level") == lv).alias(f"cnt_{lv.lower()}") for lv in LEVELS],
        *[count_if(F.col("component") == c).alias(f"cnt_{c.lower()}") for c in COMPONENTS],
        count_if(~F.col("component").isin(COMPONENTS)).alias("cnt_other_component"),
        F.countDistinct("template").cast("int").alias("distinct_templates"),
        F.countDistinct("node").cast("int").alias("distinct_nodes"),
        count_if(F.col("is_alert")).alias("alert_count"),
    )

    # label: is there an alert on the same midplane in the NEXT hour?
    hours_before_alert = (hourly.filter("alert_count > 0")
                          .select("midplane", (F.col("hour_start") - F.expr("INTERVAL 1 HOUR")).alias("hour_start"))
                          .withColumn("alert_next_hour", F.lit(True)))
    return (hourly.join(hours_before_alert, ["midplane", "hour_start"], "left")
            .fillna(False, subset=["alert_next_hour"])
            .withColumn("quarter", F.lit(quarter)))
