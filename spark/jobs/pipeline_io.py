"""I/O helpers shared by the Spark jobs: paths, HDFS checks, JDBC and audit records."""
import json
import os

from pyspark.sql import SparkSession

HDFS_URI = os.environ.get("HDFS_URI", "hdfs://hdfs-namenode:8020")
RAW = f"{HDFS_URI}/data/raw/bgl"
PROCESSED = f"{HDFS_URI}/data/processed/bgl"
CURATED = f"{HDFS_URI}/data/curated/bgl_features"

JDBC_URL = os.environ.get("SERVING_DB_JDBC_URL", "jdbc:postgresql://serving-db:5432/serving")
JDBC_USER = os.environ.get("SERVING_ETL_USER", "etl_writer")
JDBC_PASSWORD = os.environ.get("SERVING_ETL_PASSWORD", "")
MAX_MALFORMED_RATIO = float(os.environ.get("MAX_MALFORMED_RATIO", "0.01"))


def month_path(root: str, year: int, month: int) -> str:
    return f"{root}/year={year:04d}/month={month:02d}"


def spark_session(app_name: str) -> SparkSession:
    return (SparkSession.builder.appName(app_name)
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.parquet.compression.codec", "snappy")
            .getOrCreate())


def hdfs_exists(spark: SparkSession, path: str) -> bool:
    jvm = spark.sparkContext._jvm
    conf = spark.sparkContext._jsc.hadoopConfiguration()
    p = jvm.org.apache.hadoop.fs.Path(path)
    return p.getFileSystem(conf).exists(p)


def jdbc_execute(spark: SparkSession, sql: str, params: list) -> int:
    """Run one SQL statement (e.g. DELETE/INSERT) in PostgreSQL through the JDBC driver."""
    jvm = spark.sparkContext._jvm
    conn = jvm.java.sql.DriverManager.getConnection(JDBC_URL, JDBC_USER, JDBC_PASSWORD)
    try:
        stmt = conn.prepareStatement(sql)
        for i, value in enumerate(params, start=1):
            if value is None:
                stmt.setNull(i, jvm.java.sql.Types.VARCHAR)
            elif isinstance(value, bool):
                stmt.setBoolean(i, value)
            elif isinstance(value, int):
                stmt.setLong(i, value)
            else:
                stmt.setString(i, str(value))
        return stmt.executeUpdate()
    finally:
        conn.close()


def audit(spark: SparkSession, stage: str, partition: str, status: str,
          rows_in=None, rows_out=None, rows_rejected=None, details=None, source=None) -> None:
    """Append one record to governance.pipeline_runs (lineage and data-quality log)."""
    try:
        jdbc_execute(
            spark,
            "INSERT INTO governance.pipeline_runs (stage, partition_key, source_file, rows_in, rows_out, "
            "rows_rejected, status, details) VALUES (?, ?, ?, ?, ?, ?, ?, CAST(? AS jsonb))",
            [stage, partition, source, rows_in, rows_out, rows_rejected, status,
             json.dumps(details or {}, default=str)],
        )
    except Exception as exc:  # never hide the job's real result because of the audit
        print(f"WARNING: could not write audit record: {exc}")
