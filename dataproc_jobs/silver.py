"""
dataproc_jobs/silver.py — Bronze Parquet → Silver Parquet (FULL LOAD compatible)
=================================================================================

What this job does:
  1. Reads Bronze Parquet from GCS (ALL brands).
     - Default mode: reads ALL dates (full load)
     - Optional mode: if --dt is provided, reads only that date partition
  2. Derives Market_code (first 3 chars of Distributor_code)
  3. Enforces the Silver schema (consistent data types)
  4. Deduplicates rows using BUSINESS KEY columns (latest load_timestamp wins)
  5. Writes deduped data to Silver Parquet in GCS (dynamic partition overwrite)

NOTE: Delta Lake removed — uses native Parquet format for Dataproc Serverless
      compatibility (no extra JARs required).
"""

# -------------------------------
# IMPORTS
# -------------------------------
import argparse
import logging
import sys
from typing import Optional, List

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    DateType, IntegerType, LongType,
    StringType, StructField, StructType, TimestampType,
)

# -------------------------------
# LOGGING SETUP
# -------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("silver")

# -------------------------------
# CONSTANTS
# -------------------------------
BRAND_FOLDERS = ["apple", "samsung", "oppo", "vivo", "oneplus"]

# -------------------------------
# BUSINESS KEY COLUMNS
# -------------------------------
BUSINESS_KEY_COLS = [
    "Market_code",
    "Distributor_code",
    "Retailer_code",
    "Store_code",
    "EAN_code",
    "date_reported",
]

# -------------------------------
# SILVER SCHEMA
# -------------------------------
SILVER_SCHEMA = StructType([
    StructField("Market_code",      StringType(),    False),
    StructField("Brand",            StringType(),    True),
    StructField("Model",            StringType(),    True),
    StructField("Distributor_code", StringType(),    False),
    StructField("Retailer_code",    StringType(),    True),
    StructField("Store_code",       StringType(),    True),
    StructField("EAN_code",         LongType(),      False),
    StructField("Currency",         StringType(),    True),
    StructField("Price",            IntegerType(),   True),
    StructField("Stock_units",      IntegerType(),   True),
    StructField("Sale_units",       IntegerType(),   True),
    StructField("date_reported",    DateType(),      False),
    StructField("file_name",        StringType(),    False),
    StructField("load_timestamp",   TimestampType(), False),
])

# -------------------------------
# ARGUMENT PARSING
# -------------------------------
def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--pipeline_bucket", required=True,
                   help="GCS bucket holding raw_data/ (Bronze) and silver/ (output)")
    p.add_argument("--dt", required=False, default=None,
                   help="Optional processing date YYYY-MM-DD. If omitted -> full load.")
    p.add_argument("--run_id", required=True,
                   help="Run id for logging/lineage")
    p.add_argument("--env", default="dv",
                   help="Environment: dv or prod")
    args, _ = p.parse_known_args()
    return args

# -------------------------------
# SPARK SESSION (Parquet — no Delta JARs needed)
# -------------------------------
def get_spark(env: str) -> SparkSession:
    return (
        SparkSession.builder
        .appName(f"mb-silver-{env}")
        .config("spark.sql.adaptive.enabled", "true")
        # Dynamic partition overwrite: only overwrite partitions that appear in the data
        # (safe for date-partitioned incremental loads)
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )

# -------------------------------
# READ BRONZE (FULL LOAD or BY DATE)
# -------------------------------
def read_bronze(spark: SparkSession, pipeline_bucket: str, dt: Optional[str]):
    """
    Reads Bronze Parquet across ALL brands by reading brand folders one-by-one,
    then UNION-ing them.

    Critical: set basePath per brand so Spark infers partition column date_reported
    from folder names like date_reported=2026-05-20.
    """
    base = f"gs://{pipeline_bucket}/raw_data/"
    dfs: List = []

    for brand in BRAND_FOLDERS:
        brand_base = f"{base}{brand}/"

        if dt:
            path = f"{brand_base}date_reported={dt}/"
            log.info("Reading Bronze brand=%s for dt=%s from: %s", brand, dt, path)
        else:
            path = f"{brand_base}date_reported=*/"
            log.info("Reading Bronze brand=%s (all partitions) from: %s", brand, path)

        try:
            df = (
                spark.read
                .option("basePath", brand_base)   # ✅ infers date_reported partition column
                .parquet(path)
            )

            # Partition columns arrive as string; cast to DateType
            df = df.withColumn("date_reported", F.col("date_reported").cast("date"))
            dfs.append(df)

        except Exception as exc:
            log.warning("Skipping brand=%s (no data / read error): %s", brand, exc)

    if not dfs:
        log.error("No Bronze data found in any brand folders.")
        return None

    # UNION all brand DataFrames into one
    final_df = dfs[0]
    for df in dfs[1:]:
        final_df = final_df.unionByName(df, allowMissingColumns=True)

    log.info("Bronze combined across brands. Total rows=%d", final_df.count())
    return final_df

# -------------------------------
# TRANSFORM + DEDUP
# -------------------------------
def transform_and_dedup(df):
    # 1) Derive Market_code from first 3 chars of Distributor_code
    df2 = df.withColumn("Market_code", F.substring(F.col("Distributor_code"), 1, 3))

    # 2) Enforce schema by casting all columns
    df3 = df2.select(
        *[F.col(f.name).cast(f.dataType).alias(f.name) for f in SILVER_SCHEMA.fields]
    )

    # 3) Deduplicate: latest load_timestamp wins for same business keys
    w = (
        Window
        .partitionBy(*BUSINESS_KEY_COLS)
        .orderBy(F.col("load_timestamp").desc(), F.col("file_name").desc())
    )

    df_dedup = (
        df3
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    log.info("After dedup — rows=%d", df_dedup.count())

    # 4) Repartition output to control file count/size
    return df_dedup.repartition(8)

# -------------------------------
# WRITE SILVER PARQUET (replaces Delta MERGE)
# -------------------------------
def write_silver(spark: SparkSession, df_deduped, silver_path: str, dt: Optional[str]):
    """
    Writes Silver data as Parquet with dynamic partition overwrite.
    - If dt is provided: only the matching date_reported partition is overwritten.
    - If dt is None (full load): ALL partitions are overwritten.
    """
    log.info("Writing Silver Parquet to: %s (dt=%s)", silver_path, dt or "all")

    (
        df_deduped.write
        .format("parquet")
        .mode("overwrite")                  # dynamic overwrite (per spark config above)
        .partitionBy("date_reported")
        .save(silver_path)
    )

    log.info("Silver Parquet written successfully at %s", silver_path)

# -------------------------------
# MAIN
# -------------------------------
def main():
    args = parse_args()
    spark = get_spark(args.env)

    log.info("Silver job starting | dt=%s | run_id=%s | env=%s | bucket=%s",
             args.dt, args.run_id, args.env, args.pipeline_bucket)

    silver_path = f"gs://{args.pipeline_bucket}/silver/mobile_brands/silver_brands_ingest"

    df = read_bronze(spark, args.pipeline_bucket, args.dt)
    if df is None or df.rdd.isEmpty():
        log.error("No Bronze data found (dt=%s). Failing Silver job.", args.dt)
        spark.stop()
        sys.exit(1)

    df_deduped = transform_and_dedup(df)
    write_silver(spark, df_deduped, silver_path, args.dt)

    spark.stop()
    log.info("Silver job completed successfully.")

if __name__ == "__main__":
    main()