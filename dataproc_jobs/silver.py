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

Why Delta Lake in Silver?
  - Parquet folders alone are hard for incremental pipelines (no ACID, no MERGE).
  - Delta gives ACID + MERGE + time travel + schema evolution support.
  - We maintain a single “latest truth” Silver table that won’t grow duplicates.

IMPORTANT FIX FOR YOUR ERROR 🔥
------------------------------
Your Bronze folder structure is:

    raw_data/
        apple/date_reported=YYYY-MM-DD/*.parquet
        samsung/date_reported=YYYY-MM-DD/*.parquet
        ...

If we do a single wildcard read like:
    spark.read.parquet(gs://bucket/raw_data/*/date_reported=*/)

Spark sometimes still detects multiple "roots" and throws:
    Conflicting directory structures detected

✅ Reliable fix:
    Read each brand folder separately:
        raw_data/apple/date_reported=*/ ...
        raw_data/samsung/date_reported=*/ ...
    Then UNION all DataFrames together.

This avoids Spark’s partition inference conflict completely.
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
from pyspark.sql.window import Window  # window functions used for dedup "latest record wins"
from pyspark.sql.types import (
    DateType, IntegerType, LongType,
    StringType, StructField, StructType, TimestampType,
)

# DeltaTable API is the most reliable way to do MERGE into a Delta path.
# This avoids relying on Spark catalog/metastore state (which may not persist in serverless).
from delta.tables import DeltaTable  # requires delta-spark package (provided via --properties)

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
# These are the brand folder names under:
# gs://<pipeline_bucket>/raw_data/<brand>/
#
# If you add new brand folders later, update this list.
BRAND_FOLDERS = ["apple", "samsung", "oppo", "vivo", "oneplus"]


# -------------------------------
# BUSINESS KEY COLUMNS
# -------------------------------
# These columns uniquely define ONE record in the business sense.
# If duplicates exist (same keys repeated), we keep ONLY the latest record
# based on load_timestamp (and file_name as tie-breaker).
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
# Same as Bronze but adds Market_code at the front.
# We enforce types to avoid inconsistency (e.g., Price sometimes as string).
SILVER_SCHEMA = StructType([
    StructField("Market_code",      StringType(),    False),  # derived ("IND", "AUS", ...)
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
    """
    Reads command-line args passed by Airflow / gcloud dataproc batches.

    pipeline_bucket (required):
      - Bronze input: gs://<pipeline_bucket>/raw_data/
      - Silver output: gs://<pipeline_bucket>/silver/...

    dt (optional):
      - If provided: read only date_reported=dt partitions
      - If not provided: read all bronze partitions (full load)

    run_id (required):
      - Used only for logging / audit (does not change processing logic)

    env (optional):
      - dv/prod label in job name
    """
    p = argparse.ArgumentParser()
    p.add_argument("--pipeline_bucket", required=True,
                   help="GCS bucket holding raw_data/ (Bronze) and silver/ (output)")
    p.add_argument("--dt", required=False, default=None,
                   help="Optional processing date YYYY-MM-DD. If omitted -> full load.")
    p.add_argument("--run_id", required=True,
                   help="Run id for logging/lineage (manual__..., scheduled__...)")
    p.add_argument("--env", default="dv",
                   help="Environment: dv or prod")
    args, _ = p.parse_known_args()
    return args


# -------------------------------
# SPARK SESSION (Delta enabled)
# -------------------------------
def get_spark(env: str) -> SparkSession:
    """
    Creates Spark session with Delta Lake extensions enabled.
    Delta extensions are needed for reading/writing Delta format reliably.
    """
    return (
        SparkSession.builder
        .appName(f"mb-silver-{env}")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )


# -------------------------------
# READ BRONZE (FULL LOAD or BY DATE) ✅ FIXED (brand-by-brand union)
# -------------------------------
def read_bronze(spark: SparkSession, pipeline_bucket: str, dt: Optional[str]):
    """
    Reads Bronze Parquet across ALL brands by reading brand folders one-by-one,
    then UNION-ing them.

    This avoids Spark’s “Conflicting directory structures” issue when using
    wildcards across multiple brand roots.

    Bronze layout:
      gs://<bucket>/raw_data/<brand>/date_reported=YYYY-MM-DD/*.parquet

    If dt is provided:
      -> reads only date_reported=<dt> partition per brand

    If dt is not provided:
      -> reads date_reported=* for ALL partitions per brand
    """
    base = f"gs://{pipeline_bucket}/raw_data/"
    dfs = []

    for brand in BRAND_FOLDERS:
        if dt:
            path = f"{base}{brand}/date_reported={dt}/"
            log.info("Reading Bronze brand=%s for dt=%s from: %s", brand, dt, path)
        else:
            path = f"{base}{brand}/date_reported=*/"
            log.info("Reading Bronze brand=%s (all partitions) from: %s", brand, path)

        try:
            df = spark.read.parquet(path)
            # NOTE: df.count() is expensive (full scan). We avoid counting per brand here.
            dfs.append(df)
        except Exception as exc:
            # If one brand is missing (no files yet), we skip but do not fail the whole job.
            log.warning("Skipping brand=%s (no data / read error): %s", brand, exc)

    if not dfs:
        log.error("No Bronze data found in any brand folders.")
        return None

    # UNION all brand DataFrames into one DataFrame
    # allowMissingColumns=True makes union safe even if one brand has slightly different columns.
    final_df = dfs[0]
    for df in dfs[1:]:
        final_df = final_df.unionByName(df, allowMissingColumns=True)

    # One final count for logging (still expensive but useful once)
    log.info("Bronze combined across brands. Total rows=%d", final_df.count())
    return final_df


# -------------------------------
# TRANSFORM (derive Market_code + enforce schema + dedup)
# -------------------------------
def transform_and_dedup(df):
    """
    Steps:
      1) Derive Market_code from Distributor_code (first 3 chars)
      2) Enforce Silver schema types
      3) Deduplicate using window function:
           - group by BUSINESS_KEY_COLS
           - sort by load_timestamp desc (latest wins)
           - keep row_number = 1
      4) Repartition for reasonable output file sizes
    """

    # 1) Market_code derivation:
    # Distributor_code examples: INDDST01 -> "IND", AUSDST01 -> "AUS"
    df2 = df.withColumn("Market_code", F.substring(F.col("Distributor_code"), 1, 3))

    # 2) Enforce schema (cast)
    # This ensures consistent types even if upstream sends "N/A" / strings in numeric fields.
    df3 = df2.select(
        *[F.col(f.name).cast(f.dataType).alias(f.name) for f in SILVER_SCHEMA.fields]
    )

    # 3) Dedup latest per business key
    # Window: group by business keys and keep latest load_timestamp
    w = (
        Window
        .partitionBy(*BUSINESS_KEY_COLS)
        .orderBy(
            F.col("load_timestamp").desc(),
            F.col("file_name").desc()
        )
    )

    df_dedup = (
        df3
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    log.info("After dedup — rows=%d", df_dedup.count())

    # 4) Repartition
    # If dataset grows, increase or tune based on file size targets.
    return df_dedup.repartition(8)


# -------------------------------
# UPSERT INTO SILVER DELTA TABLE (path-based)
# -------------------------------
def upsert_to_silver(spark: SparkSession, df_deduped, silver_path: str):
    """
    Upserts deduplicated data into a Delta table stored at silver_path.

    Why path-based DeltaTable?
      - Serverless runs may not have persistent metastore/catalog.
      - DeltaTable.isDeltaTable(path) is reliable across runs.

    Logic:
      - If silver_path is not a Delta table yet -> create (overwrite).
      - Else -> MERGE (upsert):
          * MATCHED and newer load_timestamp -> update
          * NOT MATCHED -> insert
    """

    # If first run: create Delta table at the path
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

    # Otherwise: MERGE into existing Delta table
    log.info("Delta table exists. Performing MERGE (upsert) into %s", silver_path)

    tgt = DeltaTable.forPath(spark, silver_path)

    # Build merge condition on business keys: t.key = s.key AND ...
    merge_condition = " AND ".join([f"t.{k} = s.{k}" for k in BUSINESS_KEY_COLS])

    (
        tgt.alias("t")
        .merge(df_deduped.alias("s"), merge_condition)
        # Only update if incoming record is newer
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

    # Where Silver Delta lives in GCS
    silver_path = f"gs://{args.pipeline_bucket}/silver/mobile_brands/silver_brands_ingest_delta"

    # 1) Read Bronze
    df = read_bronze(spark, args.pipeline_bucket, args.dt)
    if df is None or df.rdd.isEmpty():
        log.error("No Bronze data found (dt=%s). Failing Silver job.", args.dt)
        spark.stop()
        sys.exit(1)

    # 2) Transform + Dedup
    df_deduped = transform_and_dedup(df)

    # 3) Upsert into Silver Delta
    upsert_to_silver(spark, df_deduped, silver_path)

    spark.stop()
    log.info("Silver job completed successfully.")


if __name__ == "__main__":
    main()