"""
dataproc_jobs/bronze.py — Transformed CSV → Bronze Parquet
===========================================================
What this job does:
  1. Reads brand CSV files from GCS (output of the Transform Cloud Run job)
  2. Adds 3 metadata columns: file_name, date_reported, load_timestamp
  3. Enforces an explicit schema (correct data types + nullability)
  4. Writes clean Parquet files to the Bronze layer, partitioned by date

This runs on a Dataproc cluster via spark-submit, triggered by Airflow.

Usage (spark-submit on Dataproc):
  spark-submit bronze.py \
    --source_bucket=dv-mb-data-bucket \
    --pipeline_bucket=dv-mb-pipeline-bucket \
    --dt=2026-05-19 \
    --run_id=scheduled__2026-05-19T010000 \
    --env=dv
"""

import argparse        # for reading command-line arguments (--dt, --env, etc.)
import logging         # for structured log messages instead of plain print()
import sys             # for sys.exit(1) — tells Airflow the job failed

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,       # for date columns (YYYY-MM-DD, no time)
    IntegerType,    # for whole numbers (Price, Stock_units, Sale_units)
    LongType,       # for large numbers that exceed int range (EAN_code barcodes)
    StringType,     # for text columns
    StructField,    # defines one column in a schema (name, type, nullable)
    StructType,     # collection of StructFields = full schema definition
    TimestampType,  # for datetime columns (load_timestamp)
)

# ---------------------------------------------------------------------------
# LOGGING SETUP
# ---------------------------------------------------------------------------
# Sets up a logger that prints timestamp + level + message to stdout.
# Dataproc captures stdout → visible in Cloud Logging / Dataproc job logs.
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("bronze")

# ---------------------------------------------------------------------------
# CONSTANTS
# ---------------------------------------------------------------------------
# All 5 brands this pipeline processes.
# The job loops through each brand and reads its CSV files separately.
BRANDS = ["Samsung", "Apple", "Oppo", "Vivo", "OnePlus"]

# ---------------------------------------------------------------------------
# BRONZE SCHEMA DEFINITION
# ---------------------------------------------------------------------------
# This defines the exact structure (column names + data types) we want in the
# Bronze layer. Instead of letting Spark guess types (inferSchema), we cast
# every column explicitly. This avoids surprises when a distributor sends
# "N/A" in a numeric column or changes their column order.
#
# nullable=False means that column MUST have a value — Spark will fail the
# job if it's null, which is a data quality gate.
BRONZE_SCHEMA = StructType([
    StructField("Brand",            StringType(),    True),   # e.g. "Samsung"
    StructField("Model",            StringType(),    True),   # e.g. "Galaxy S24"
    StructField("Distributor_code", StringType(),    True),   # e.g. "INDDST01"
    StructField("Retailer_code",    StringType(),    True),   # e.g. "INDRET01"
    StructField("Store_code",       StringType(),    True),   # e.g. "INDSTR001"
    StructField("EAN_code",         LongType(),      True),   # barcode — too big for IntegerType
    StructField("Currency",         StringType(),    True),   # e.g. "INR", "AUD"
    StructField("Price",            IntegerType(),   True),   # selling price
    StructField("Stock_units",      IntegerType(),   True),   # units in stock
    StructField("Sale_units",       IntegerType(),   True),   # units sold
    StructField("date_reported",    DateType(),      False),  # date parsed from filename — MUST exist
    StructField("file_name",        StringType(),    False),  # source filename — MUST exist (for lineage)
    StructField("load_timestamp",   TimestampType(), False),  # when this job ran — MUST exist
])


# ---------------------------------------------------------------------------
# ARGUMENT PARSING
# ---------------------------------------------------------------------------
def parse_args():
    """
    Reads the arguments that Airflow passes to spark-submit.
    Example: --dt=2026-05-19 --pipeline_bucket=dv-mb-pipeline-bucket

    parse_known_args() is used instead of parse_args() so that if Airflow
    sends extra flags we don't know about, they are silently ignored instead
    of crashing the job.
    """
    p = argparse.ArgumentParser()
    p.add_argument("--source_bucket",   required=True,
                   help="GCS bucket holding transformed/ CSVs (data bucket)")
    p.add_argument("--pipeline_bucket", required=True,
                   help="GCS bucket for Bronze output raw_data/ (pipeline bucket)")
    p.add_argument("--dt",              required=True,
                   help="Processing date in YYYY-MM-DD format (Airflow {{ ds }})")
    p.add_argument("--run_id",          required=True,
                   help="Airflow run_id — used for lineage / audit logs")
    p.add_argument("--env",             default="dv",
                   help="Environment: dv or prod")
    args, _ = p.parse_known_args()   # _ captures any extra/unknown args — we discard them
    return args


# ---------------------------------------------------------------------------
# SPARK SESSION
# ---------------------------------------------------------------------------
def get_spark(env: str) -> SparkSession:
    """
    Creates (or reuses) the Spark session on the Dataproc cluster.
    On Dataproc, SparkSession.builder connects to the already-running
    cluster automatically — we don't need to specify master or executor config.

    partitionOverwriteMode=dynamic means when we write a partition (e.g.
    date_reported=2026-05-19), only THAT partition is overwritten. Other
    date partitions are left untouched. This makes reruns safe.
    """
    return (
        SparkSession.builder
        .appName(f"mb-bronze-{env}")   # job name visible in Spark UI and logs
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")  # safe reruns
        .config("spark.sql.adaptive.enabled", "true")   # Spark auto-tunes join strategies
        .getOrCreate()
    )


# ---------------------------------------------------------------------------
# PROCESS ONE BRAND
# ---------------------------------------------------------------------------
def process_brand(spark: SparkSession, args, brand: str) -> int:
    """
    Reads all CSV files for one brand on the given date, adds metadata columns,
    enforces the Bronze schema, and writes Parquet to GCS.

    Returns the number of rows written (0 if no files found for this brand).
    """

    # Convert YYYY-MM-DD → YYYYMMDD to match the filename convention
    # e.g. "2026-05-19" → "20260519"
    date_nodash = args.dt.replace("-", "")

    # Build the GCS glob path for this brand's CSVs on this date.
    # The transform job writes files like: INDDST01_20260519.csv
    # The * wildcard matches any distributor code prefix.
    src = f"gs://{args.source_bucket}/transformed/{brand.lower()}/*_{date_nodash}.csv"
    log.info("Reading: %s", src)

    # Try reading — if no files match the glob, Spark raises an exception
    try:
        df = spark.read.option("header", "true").csv(src)
    except Exception as exc:
        # No files found for this brand today — warn but don't fail the whole job
        log.warning("No data found for brand=%s: %s", brand, exc)
        return 0

    # isEmpty() triggers a count — needed to detect truly empty DataFrames
    if df.rdd.isEmpty():
        log.warning("Empty dataset for brand=%s — skipping.", brand)
        return 0

    # ── STEP 1: Extract the source filename ───────────────────────────────
    # input_file_name() returns the full GCS path for each row, e.g.:
    #   gs://bucket/transformed/apple/AUSDST01_20260519.csv
    # substring_index(..., "/", -1) takes everything after the last "/" → filename only
    df1 = df.withColumn(
        "file_name",
        F.substring_index(F.input_file_name(), "/", -1)
    )

    # ── STEP 2: Extract date_reported from the filename ───────────────────
    # Filename format: AUSDST01_20260519.csv
    # substring_index(file_name, "_", -1) → "20260519.csv"
    # substring_index("20260519.csv", ".", 1) → "20260519"
    # to_date("20260519", "yyyyMMdd") → DateType 2026-05-19
    df2 = df1.withColumn(
        "date_reported",
        F.to_date(
            F.substring_index(F.substring_index(F.col("file_name"), "_", -1), ".", 1),
            "yyyyMMdd"
        )
    )

    # ── STEP 3: Add load timestamp ─────────────────────────────────────────
    # Records when this Spark job processed this file.
    # Used in Silver for deduplication — later load_timestamp wins on duplicates.
    df3 = df2.withColumn("load_timestamp", F.current_timestamp())

    # ── STEP 4: Enforce explicit schema ───────────────────────────────────
    # Cast every column to the type defined in BRONZE_SCHEMA.
    # This is a list comprehension that iterates over each field definition
    # and creates a col().cast() expression, then selects all of them.
    # Result: all columns are in the correct order with correct types.
    df_bronze = df3.select(
        *[F.col(f.name).cast(f.dataType).alias(f.name) for f in BRONZE_SCHEMA.fields]
    )

    # ── STEP 5: Write Bronze Parquet ──────────────────────────────────────
    # Output path: gs://<pipeline_bucket>/raw_data/apple/
    # Parquet is columnar format — much faster to query than CSV.
    # partitionBy("date_reported") creates subfolders like date_reported=2026-05-19/
    # mode("append") adds to existing data without deleting other date partitions.
    dest = f"gs://{args.pipeline_bucket}/raw_data/{brand.lower()}/"
    count = df_bronze.count()   # triggers actual Spark computation

    df_bronze.write \
        .mode("append") \
        .partitionBy("date_reported") \
        .parquet(dest)

    log.info("Bronze written: %s | rows=%d | partition=date_reported=%s",
             dest, count, args.dt)
    return count


# ---------------------------------------------------------------------------
# MAIN — Entry Point
# ---------------------------------------------------------------------------
def main():
    args  = parse_args()
    spark = get_spark(args.env)
    log.info("Bronze job starting | dt=%s | run_id=%s | env=%s",
             args.dt, args.run_id, args.env)
    log.info("source_bucket=%s | pipeline_bucket=%s",
             args.source_bucket, args.pipeline_bucket)

    total  = 0    # total rows written across all brands
    failed = []   # list of brands that raised an exception

    # Process each brand independently so one brand's failure doesn't block others
    for brand in BRANDS:
        try:
            total += process_brand(spark, args, brand)
        except Exception as exc:
            log.error("Bronze FAILED for brand=%s: %s", brand, exc)
            failed.append(brand)   # track which brands failed

    spark.stop()   # release Dataproc cluster resources

    # If any brand failed → exit with code 1 so Airflow marks this task as FAILED
    if failed:
        log.error("Bronze completed with failures for brands: %s", failed)
        sys.exit(1)

    log.info("Bronze complete | total_rows=%d across all brands", total)


# Python convention: only run main() when this file is executed directly
# (not when it's imported as a module)
if __name__ == "__main__":
    main()
