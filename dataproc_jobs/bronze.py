"""
dataproc_jobs/bronze.py — FULL LOAD VERSION (process ALL files)

What changed from your old version:
✅ Reads ALL CSV files (no date filtering)
✅ Still extracts date from file name
✅ Safe because you archive files after processing
"""

# -------------------------------
# IMPORTS
# -------------------------------
import argparse        # to read command-line arguments
import logging         # for printing logs
import sys             # to exit job on failure

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType, IntegerType, LongType,
    StringType, StructField, StructType, TimestampType
)

# -------------------------------
# LOGGING SETUP
# -------------------------------
# This prints logs in nice format in Dataproc logs
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("bronze")

# -------------------------------
# CONSTANTS
# -------------------------------
# List of brands (folders in GCS)
BRANDS = ["Samsung", "Apple", "Oppo", "Vivo", "OnePlus"]

# -------------------------------
# SCHEMA (FINAL CLEAN STRUCTURE)
# -------------------------------
# We enforce data types — very important for consistency
BRONZE_SCHEMA = StructType([
    StructField("Brand",            StringType(),    True),
    StructField("Model",            StringType(),    True),
    StructField("Distributor_code", StringType(),    True),
    StructField("Retailer_code",    StringType(),    True),
    StructField("Store_code",       StringType(),    True),
    StructField("EAN_code",         LongType(),      True),
    StructField("Currency",         StringType(),    True),
    StructField("Price",            IntegerType(),   True),
    StructField("Stock_units",      IntegerType(),   True),
    StructField("Sale_units",       IntegerType(),   True),

    # ✅ Metadata columns we generate
    StructField("date_reported",    DateType(),      False),  # date from file name
    StructField("file_name",        StringType(),    False),  # source file
    StructField("load_timestamp",   TimestampType(), False),  # load time
])

# -------------------------------
# ARGUMENT PARSING
# -------------------------------
def parse_args():
    """
    Reads arguments passed from gcloud dataproc command
    """
    p = argparse.ArgumentParser()

    # GCS bucket where transformed CSV files exist
    p.add_argument("--source_bucket", required=True)

    # GCS bucket where Bronze data will be written
    p.add_argument("--pipeline_bucket", required=True)

    # Only for logging
    p.add_argument("--run_id", required=True)

    # Environment (dv/prod)
    p.add_argument("--env", default="dv")

    args, _ = p.parse_known_args()
    return args


# -------------------------------
# SPARK SESSION
# -------------------------------
def get_spark(env: str):
    """
    Creates Spark session on Dataproc
    """
    return (
        SparkSession.builder
        .appName(f"mb-bronze-{env}")

        # Allows overwriting only specific partitions safely
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")

        # Spark auto optimization (AQE)
        .config("spark.sql.adaptive.enabled", "true")

        .getOrCreate()
    )


# -------------------------------
# PROCESS EACH BRAND
# -------------------------------
def process_brand(spark, args, brand: str):
    """
    Reads ALL CSV files for a brand and writes Bronze parquet
    """

    # ✅ STEP 1: Read ALL files (no date filtering)
    src = f"gs://{args.source_bucket}/transformed/{brand.lower()}/*.csv"
    log.info("Reading: %s", src)

    try:
        df = spark.read.option("header", "true").csv(src)
    except Exception as exc:
        log.warning("No data found for brand=%s: %s", brand, exc)
        return 0

    # If no data → skip
    if df.rdd.isEmpty():
        log.warning("Empty dataset for brand=%s", brand)
        return 0

    # ✅ STEP 2: Extract file name
    df1 = df.withColumn(
        "file_name",
        F.substring_index(F.input_file_name(), "/", -1)
    )

    # ✅ STEP 3: Extract date from file name
    # Example:
    # AUSDST01_20260519.csv → 2026-05-19
    df2 = df1.withColumn(
        "date_reported",
        F.to_date(
            F.substring_index(
                F.substring_index(F.col("file_name"), "_", -1), ".", 1
            ),
            "yyyyMMdd"
        )
    )

    # ✅ STEP 4: Add load timestamp
    df3 = df2.withColumn(
        "load_timestamp",
        F.current_timestamp()
    )

    # ✅ STEP 5: Enforce schema (important for consistency)
    df_bronze = df3.select(
        *[
            F.col(f.name).cast(f.dataType).alias(f.name)
            for f in BRONZE_SCHEMA.fields
        ]
    )

    # ✅ STEP 6: Write to Bronze (Parquet format)
    dest = f"gs://{args.pipeline_bucket}/raw_data/{brand.lower()}/"

    count = df_bronze.count()   # triggers execution

    df_bronze.write \
        .mode("append") \
        .partitionBy("date_reported") \
        .parquet(dest)

    log.info("Written: %s | rows=%d", dest, count)

    return count


# -------------------------------
# MAIN FUNCTION
# -------------------------------
def main():
    args = parse_args()
    spark = get_spark(args.env)

    log.info("Bronze job started | run_id=%s", args.run_id)
    log.info("source_bucket=%s | pipeline_bucket=%s",
             args.source_bucket, args.pipeline_bucket)

    total_rows = 0

    # Loop through each brand
    for brand in BRANDS:
        try:
            total_rows += process_brand(spark, args, brand)
        except Exception as exc:
            log.error("FAILED for brand=%s: %s", brand, exc)
            sys.exit(1)  # fail job if any brand fails

    spark.stop()

    log.info("Bronze complete | total_rows=%d", total_rows)


# -------------------------------
# ENTRY POINT
# -------------------------------
if __name__ == "__main__":
    main()