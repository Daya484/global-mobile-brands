"""
dataproc_jobs/silver.py — Bronze Parquet → Silver Delta Lake (FULL LOAD compatible)
====================================================================================

What this job does:
  1. Reads Bronze Parquet from GCS (ALL brands).
     - Default mode: reads ALL dates (full load)
     - Optional mode: if --dt is provided, reads only that date partition
  2. Derives Market_code (first 3 chars of Distributor_code)
  3. Enforces the Silver schema (consistent data types)
  4. Deduplicates rows using BUSINESS KEY columns (latest load_timestamp wins)
  5. Upserts (MERGE) the deduped batch into a Delta Lake table in GCS

Why Delta Lake in Silver?
  - Parquet alone cannot support MERGE (no ACID)
  - Delta supports:
      ✅ Update
      ✅ Insert
      ✅ Time travel
  - Ensures no duplicates across reruns

IMPORTANT FIX (VERY IMPORTANT 🔥)
--------------------------------
Your Bronze folder structure is:

    raw_data/
        samsung/date_reported=2026-05-20/
        apple/date_reported=2026-05-20/
        ...

Spark expects a "single table root" for partition discovery.

❌ If you read raw_data/*/ → Spark throws:
    "Conflicting directory structures"

✅ Fix:
    Read ONLY partition folders:
        raw_data/*/date_reported=*/
    AND define:
        basePath = raw_data/

This tells Spark:
    - root = raw_data
    - partition column = date_reported
"""

# -------------------------------
# IMPORTS
# -------------------------------
import argparse
import logging
import sys
from typing import Optional

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

    p.add_argument("--pipeline_bucket", required=True)
    p.add_argument("--dt", required=False, default=None)
    p.add_argument("--run_id", required=True)
    p.add_argument("--env", default="dv")

    args, _ = p.parse_known_args()
    return args


# -------------------------------
# SPARK SESSION
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
# READ BRONZE ✅ FIXED VERSION
# -------------------------------
def read_bronze(spark: SparkSession, pipeline_bucket: str, dt: Optional[str]):

    base_path = f"gs://{pipeline_bucket}/raw_data/"

    if dt:
        src = f"{base_path}*/date_reported={dt}/"
        log.info("Reading Bronze for dt=%s from: %s", dt, src)
    else:
        src = f"{base_path}*/date_reported=*/"
        log.info("Reading ALL Bronze partitions from: %s", src)

    try:
        df = (
            spark.read
            .option("basePath", base_path)   # ✅ critical fix
            .parquet(src)
        )

        log.info("Bronze read successful — rows=%d", df.count())
        return df

    except Exception as exc:
        log.error("Cannot read Bronze: %s", exc)
        return None


# -------------------------------
# TRANSFORM + DEDUP
# -------------------------------
def transform_and_dedup(df):

    # 1. Derive Market_code
    df2 = df.withColumn(
        "Market_code",
        F.substring(F.col("Distributor_code"), 1, 3)
    )

    # 2. Enforce schema
    df3 = df2.select(
        *[F.col(f.name).cast(f.dataType).alias(f.name) for f in SILVER_SCHEMA.fields]
    )

    # 3. Dedup logic (latest record wins)
    window_spec = (
        Window
        .partitionBy(*BUSINESS_KEY_COLS)
        .orderBy(
            F.col("load_timestamp").desc(),
            F.col("file_name").desc()
        )
    )

    df_deduped = (
        df3
        .withColumn("_rn", F.row_number().over(window_spec))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    log.info("After dedup — rows=%d", df_deduped.count())

    return df_deduped.repartition(8)


# -------------------------------
# UPSERT TO DELTA
# -------------------------------
def upsert_to_silver(spark: SparkSession, df_deduped, silver_path: str):

    if not DeltaTable.isDeltaTable(spark, silver_path):
        log.info("First run — creating Silver Delta table")

        (
            df_deduped.write
            .format("delta")
            .mode("overwrite")
            .partitionBy("date_reported")
            .save(silver_path)
        )

        log.info("Delta table created ✅")
        return

    log.info("Delta exists — performing MERGE")

    target = DeltaTable.forPath(spark, silver_path)

    merge_condition = " AND ".join([f"t.{k} = s.{k}" for k in BUSINESS_KEY_COLS])

    (
        target.alias("t")
        .merge(df_deduped.alias("s"), merge_condition)
        .whenMatchedUpdate(
            condition="s.load_timestamp > t.load_timestamp",
            set={c: f"s.{c}" for c in df_deduped.columns}
        )
        .whenNotMatchedInsert(values={c: f"s.{c}" for c in df_deduped.columns})
        .execute()
    )

    log.info("MERGE completed ✅")


# -------------------------------
# MAIN
# -------------------------------
def main():

    args = parse_args()
    spark = get_spark(args.env)

    log.info("Silver job starting | dt=%s", args.dt)

    silver_path = f"gs://{args.pipeline_bucket}/silver/mobile_brands/silver_brands_ingest_delta"

    # Read Bronze
    df = read_bronze(spark, args.pipeline_bucket, args.dt)

    if df is None or df.rdd.isEmpty():
        log.error("No Bronze data — exiting")
        sys.exit(1)

    # Transform
    df_deduped = transform_and_dedup(df)

    # Upsert
    upsert_to_silver(spark, df_deduped, silver_path)

    spark.stop()
    log.info("Silver job completed ✅")


if __name__ == "__main__":
    main()