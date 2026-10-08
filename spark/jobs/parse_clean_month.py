"""Monthly Spark job: raw log lines -> cleaned, structured Parquet.

    spark-submit parse_clean_month.py <year> <month>

Reads   hdfs:/data/raw/bgl/year=YYYY/month=MM/
Writes  hdfs:/data/processed/bgl/year=YYYY/month=MM/   (Parquet, overwritten -> idempotent)

Data-quality gate: the job fails (exit code 2) if more than MAX_MALFORMED_RATIO
of the lines cannot be parsed, so Airflow stops the pipeline for that month.
"""
import sys

from bgl_transform import parse_and_clean
from pipeline_io import (MAX_MALFORMED_RATIO, PROCESSED, RAW, audit, hdfs_exists,
                         month_path, spark_session)


def main(year: int, month: int) -> int:
    partition = f"{year:04d}-{month:02d}"
    spark = spark_session(f"parse_clean_{partition}")
    src, dst = month_path(RAW, year, month), month_path(PROCESSED, year, month)

    if not hdfs_exists(spark, src):
        print(f"ERROR: raw partition {src} does not exist")
        audit(spark, "parse_clean", partition, "failed", details={"error": "raw partition missing"})
        return 1

    clean, stats = parse_and_clean(spark.read.text(src))
    print(f"[{partition}] quality stats: {stats}")

    if stats["malformed_ratio"] > MAX_MALFORMED_RATIO:
        print(f"ERROR: malformed ratio {stats['malformed_ratio']:.4f} > {MAX_MALFORMED_RATIO}")
        audit(spark, "parse_clean", partition, "rejected", stats["rows_in"], 0, stats["malformed"],
              stats, source=src)
        return 2

    clean.coalesce(4).write.mode("overwrite").parquet(dst)
    audit(spark, "parse_clean", partition, "success", stats["rows_in"], stats["rows_out"],
          stats["malformed"], stats, source=src)
    print(f"[{partition}] wrote {stats['rows_out']:,} rows to {dst}")
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main(int(sys.argv[1]), int(sys.argv[2])))
