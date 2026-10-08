"""Quarterly Spark job: cleaned events -> hourly ML features -> PostgreSQL.

    spark-submit aggregate_quarter.py <year> <quarter 1-4>

Reads   hdfs:/data/processed/bgl/year=YYYY/month=MM/  for the months of the quarter
Writes  hdfs:/data/curated/bgl_features/quarter=YYYYQn/  (Parquet, overwritten)
Loads   features.midplane_hourly in the serving database (rows of this quarter are
        deleted first, so a re-run replaces the quarter instead of duplicating it)
"""
import sys

from bgl_transform import build_hourly_features
from pipeline_io import (CURATED, JDBC_PASSWORD, JDBC_URL, JDBC_USER, PROCESSED, audit,
                         hdfs_exists, jdbc_execute, month_path, spark_session)


def main(year: int, quarter: int) -> int:
    label = f"{year:04d}Q{quarter}"
    spark = spark_session(f"aggregate_{label}")

    months = [3 * (quarter - 1) + i for i in (1, 2, 3)]
    paths = [month_path(PROCESSED, year, m) for m in months]
    existing = [p for p in paths if hdfs_exists(spark, f"{p}/_SUCCESS")]
    print(f"[{label}] processed partitions found: {existing}")
    if not existing:
        print("ERROR: no processed data for this quarter")
        audit(spark, "aggregate_deliver", label, "failed", details={"error": "no processed partitions"})
        return 1

    events = spark.read.parquet(*existing)
    features = build_hourly_features(events, label).cache()
    rows_in, rows_out = events.count(), features.count()
    positives = features.filter("alert_next_hour").count()

    # curated zone (bulk access, e.g. for data scientists)
    features.write.mode("overwrite").parquet(f"{CURATED}/quarter={label}")

    # serving database (one versioned feature set per quarter)
    deleted = jdbc_execute(spark, "DELETE FROM features.midplane_hourly WHERE quarter = ?", [label])
    (features.write.format("jdbc")
        .option("url", JDBC_URL).option("dbtable", "features.midplane_hourly")
        .option("user", JDBC_USER).option("password", JDBC_PASSWORD)
        .option("driver", "org.postgresql.Driver").option("batchsize", 5000)
        .mode("append").save())

    details = {"months_used": existing, "feature_rows": rows_out, "positive_labels": positives,
               "replaced_rows": deleted}
    audit(spark, "aggregate_deliver", label, "success", rows_in, rows_out, 0, details)
    print(f"[{label}] {rows_in:,} events -> {rows_out:,} feature rows "
          f"({positives:,} with alert in next hour); replaced {deleted} old rows")
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main(int(sys.argv[1]), int(sys.argv[2])))
