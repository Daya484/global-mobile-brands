"""
dataproc_jobs/silver.py — Bronze Parquet → Silver Delta Lake (FULL LOAD compatible)
=================================================================================

What this job does:
  1. Reads Bronze Parquet from GCS (ALL brands).
     - Default mode: reads ALL dates (full load)
     - Optional mode: if --dt is provided, reads only that date partition
  2. Derives Market_code (first 3 chars of Distributor_code)
  3. Enforces the Silver schema (consistent data types)
  4. Deduplicates rows using BUSINESS KEY columns (latest load_timestamp wins)
  5. Upserts (MERGE) the deduped batch into a Delta Lake table in GCS

Why you got the error: date_reported missing
--------------------------------------------
Bronze writes Parquet partitioned by date_reported:
  raw_data/<brand>/date_reported=YYYY-MM-DD/part-*.parquet

Spark often stores partition values in the DIRECTORY name, not inside the Parquet files.
So `date_reported` may not appear when reading unless Spark is told the basePath.
Spark docs note that when reading partition directories, you should set basePath to get
partition columns inferred. 【3-18db02】
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

from delta.tables import DeltaTable

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
# Brand folders under gs://<pipeline_bucket>/raw_data/<brand>/
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
# SPARK SESSION (Delta enabled)
# -------------------------------
def get_spark(env: str) -> SparkSession:
    return (
        SparkSession.builder
        .appName(f"mb-silver-{env}")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )

# -------------------------------
# READ BRONZE (FULL LOAD or BY DATE) ✅ FIXED
# -------------------------------
def read_bronze(spark: SparkSession, pipeline_bucket: str, dt: Optional[str]):
    """
    Reads Bronze Parquet across ALL brands by reading brand folders one-by-one,
    then UNION-ing them.

    ✅ Critical: set basePath per brand so Spark infers partition column date_reported
       from folder names like date_reported=2026-05-20. 【3-18db02】
    """
    base = f"gs://{pipeline_bucket}/raw_data/"
    dfs: List = []

    for brand in BRAND_FOLDERS:
        # basePath must be the "table root" for that brand folder
        brand_base = f"{base}{brand}/"

        # Read either one partition or all partitions
        if dt:
            path = f"{brand_base}date_reported={dt}/"
            log.info("Reading Bronze brand=%s for dt=%s from: %s", brand, dt, path)
        else:
            path = f"{brand_base}date_reported=*/"
            log.info("Reading Bronze brand=%s (all partitions) from: %s", brand, path)

        try:
            df = (
                spark.read
                # ✅ makes Spark add partition column(s) like date_reported
                .option("basePath", brand_base)
                .parquet(path)
            )

            # Partition columns usually arrive as string; cast to DateType
            # so Silver schema enforcement works.
            df = df.withColumn("date_reported", F.col("date_reported").cast("date"))

            dfs.append(df)

        except Exception as exc:
            log.warning("Skipping brand=%s (no data / read error): %s", brand, exc)

    if not dfs:
        log.error("No Bronze data found in any brand folders.")
        return None

    # UNION all brand DataFrames into one DataFrame
    # allowMissingColumns=True makes union safe if one brand is missing a column.
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
# UPSERT INTO SILVER DELTA TABLE (path-based)
# -------------------------------
def upsert_to_silver(spark: SparkSession, df_deduped, silver_path: str):
    # First run: create Delta table
    if not DeltaTable.isDeltaTable(spark, silver_path):
        log.info("First run: creating Silver Delta table at %s", silver_path)
        (
            df_deduped.write
            .format("delta")
            .mode("overwrite")
            .partitionBy("date_reported")
            .save(silver_path)
        )
        log.info("Silver Delta created at %s | rows=%d", silver_path, df_deduped.count())
        return

    # Subsequent runs: MERGE
    log.info("Delta table exists. Performing MERGE (upsert) into %s", silver_path)

    tgt = DeltaTable.forPath(spark, silver_path)
    merge_condition = " AND ".join([f"t.{k} = s.{k}" for k in BUSINESS_KEY_COLS])

    (
        tgt.alias("t")
        .merge(df_deduped.alias("s"), merge_condition)
        .whenMatchedUpdate(
            condition="s.load_timestamp > t.load_timestamp",
            set={c: f"s.{c}" for c in df_deduped.columns}
        )
        .whenNotMatchedInsert(values={c: f"s.{c}" for c in df_deduped.columns})
        .execute()
    )

    log.info("Silver MERGE completed successfully.")

# -------------------------------
# MAIN
# -------------------------------
def main():
    args = parse_args()
    spark = get_spark(args.env)

    log.info("Silver job starting | dt=%s | run_id=%s | env=%s | bucket=%s",
             args.dt, args.run_id, args.env, args.pipeline_bucket)

    silver_path = f"gs://{args.pipeline_bucket}/silver/mobile_brands/silver_brands_ingest_delta"

    df = read_bronze(spark, args.pipeline_bucket, args.dt)
    if df is None or df.rdd.isEmpty():
        log.error("No Bronze data found (dt=%s). Failing Silver job.", args.dt)
        spark.stop()
        sys.exit(1)

    df_deduped = transform_and_dedup(df)
    upsert_to_silver(spark, df_deduped, silver_path)

    spark.stop()
    log.info("Silver job completed successfully.")

if __name__ == "__main__":
    main()