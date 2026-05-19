"""
dataproc_jobs/silver.py — Bronze Parquet → Silver Delta Lake
=============================================================
What this job does:
  1. Reads ALL brands' Bronze Parquet from GCS for today's date
  2. Derives Market_code (first 3 chars of Distributor_code)
  3. Enforces the Silver schema (same as Bronze + Market_code column)
  4. Deduplicates rows by business key columns (keeps the most recent record)
  5. Repartitions for optimal Parquet file sizes
  6. EITHER creates the Silver Delta table (first ever run)
     OR merges new data into the existing table (subsequent runs)

Delta Lake gives us ACID transactions, time travel, and efficient MERGE
(upsert) support — much better than plain Parquet for an incremental pipeline.

Usage (spark-submit on Dataproc):
  spark-submit silver.py \
    --pipeline_bucket=dv-mb-pipeline-bucket \
    --dt=2026-05-19 \
    --run_id=scheduled__2026-05-19T010000 \
    --env=dv
"""

import argparse
import logging
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window   # Window = defines a "group + order" for row-level functions
from pyspark.sql.types import (
    DateType, IntegerType, LongType,
    StringType, StructField, StructType, TimestampType,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("silver")

# ---------------------------------------------------------------------------
# BUSINESS KEY COLUMNS
# ---------------------------------------------------------------------------
# These columns together uniquely identify ONE sale/stock record.
# If the same combination appears more than once (e.g. because a distributor
# sent the file twice, or we re-ran the pipeline), we keep only the latest one.
#
# Think of it like a compound primary key in a database:
#   Market + Distributor + Retailer + Store + Product + Date = 1 unique record
BUSINESS_KEY_COLS = [
    "Market_code",       # which country market (first 3 chars of Distributor_code, e.g. "IND")
    "Distributor_code",  # the distributor (e.g. "INDDST01")
    "Retailer_code",     # the retailer within that distributor
    "Store_code",        # the specific store
    "EAN_code",          # the product barcode (uniquely identifies the model/variant)
    "date_reported",     # the date of the transaction
]

# ---------------------------------------------------------------------------
# SILVER SCHEMA DEFINITION
# ---------------------------------------------------------------------------
# Same as Bronze but adds Market_code (derived column) at the front.
# Market_code is not in the raw CSV — we compute it from Distributor_code.
SILVER_SCHEMA = StructType([
    StructField("Market_code",      StringType(),    False),  # derived: "IND", "AUS", etc.
    StructField("Brand",            StringType(),    True),
    StructField("Model",            StringType(),    True),
    StructField("Distributor_code", StringType(),    False),  # required — it's part of the key
    StructField("Retailer_code",    StringType(),    True),
    StructField("Store_code",       StringType(),    True),
    StructField("EAN_code",         LongType(),      False),  # required — it's part of the key
    StructField("Currency",         StringType(),    True),
    StructField("Price",            IntegerType(),   True),
    StructField("Stock_units",      IntegerType(),   True),
    StructField("Sale_units",       IntegerType(),   True),
    StructField("date_reported",    DateType(),      False),  # required — it's part of the key
    StructField("file_name",        StringType(),    False),  # required — for audit/lineage
    StructField("load_timestamp",   TimestampType(), False),  # required — used for dedup ordering
])


# ---------------------------------------------------------------------------
# ARGUMENT PARSING
# ---------------------------------------------------------------------------
def parse_args():
    """
    Reads command-line arguments passed by Airflow via spark-submit.
    parse_known_args() silently ignores any extra flags Airflow sends
    that this script doesn't know about (e.g. --source_bucket).
    """
    p = argparse.ArgumentParser()
    p.add_argument("--pipeline_bucket", required=True,
                   help="GCS bucket holding raw_data/ (Bronze) and silver/ (output)")
    p.add_argument("--dt",              required=True,
                   help="Processing date YYYY-MM-DD (Airflow {{ ds }})")
    p.add_argument("--run_id",          required=True,
                   help="Airflow run_id for logging/lineage")
    p.add_argument("--env",             default="dv",
                   help="Environment: dv or prod")
    args, _ = p.parse_known_args()
    return args


# ---------------------------------------------------------------------------
# SPARK SESSION — with Delta Lake extensions
# ---------------------------------------------------------------------------
def get_spark(env: str) -> SparkSession:
    """
    Creates the Spark session with Delta Lake support.

    Delta Lake needs two config entries to work:
      - spark.sql.extensions: registers the Delta SQL commands (MERGE INTO, etc.)
      - spark.sql.catalog.spark_catalog: replaces Spark's default catalog
        so that table operations (CREATE TABLE, DROP TABLE, etc.) go through Delta.

    Note: The Delta Lake JAR (delta-spark_2.12:3.2.0) is already loaded at
    the Dataproc cluster level via software_config.properties in the DAG —
    so we don't need to add it again here.
    """
    return (
        SparkSession.builder
        .appName(f"mb-silver-{env}")
        # Register Delta SQL extensions so we can use MERGE INTO syntax
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        # Use Delta catalog instead of Hive catalog for table management
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )


# ---------------------------------------------------------------------------
# READ ALL BRANDS' BRONZE DATA FOR TODAY'S DATE
# ---------------------------------------------------------------------------
def read_bronze(spark: SparkSession, pipeline_bucket: str, dt: str):
    """
    Reads Bronze Parquet for ALL brands in one single DataFrame.

    Bronze writes per-brand folders partitioned by date:
      gs://<bucket>/raw_data/samsung/date_reported=2026-05-19/*.parquet
      gs://<bucket>/raw_data/apple/date_reported=2026-05-19/*.parquet
      ... etc.

    The wildcard * matches all brand folders at once, so we get
    Samsung + Apple + Oppo + Vivo + OnePlus all in one read call.
    Spark's partition discovery reads only the matching date partition.
    """
    # * wildcard across brand folders, specific date partition
    src = f"gs://{pipeline_bucket}/raw_data/*/date_reported={dt}/"
    log.info("Reading Bronze (all brands) from: %s", src)
    try:
        return spark.read.parquet(src)
    except Exception as exc:
        log.error("Cannot read Bronze for dt=%s: %s", dt, exc)
        return None


# ---------------------------------------------------------------------------
# TRANSFORM: DERIVE MARKET_CODE + SCHEMA + DEDUP + REPARTITION
# ---------------------------------------------------------------------------
def transform(df):
    """
    Applies three transformations to the raw Bronze data:

    1. Derive Market_code:
       Distributor_code format is: <3-char market><distributor number>
       e.g. "INDDST01" → first 3 chars → "IND"
       substring(col, start_position, length)  — positions are 1-indexed in Spark

    2. Enforce Silver schema:
       Cast every column to the declared type, ensuring consistent types
       across all brands (some CSVs may have slightly different representations).

    3. Deduplicate by business keys:
       If the same Market+Distributor+Retailer+Store+EAN+Date appears more
       than once, keep only the row with the latest load_timestamp.
       This handles re-runs (same file processed twice) and late-arriving files.

       Window function approach:
         - Partition by all business key columns (group rows with same key)
         - Order by load_timestamp DESC, file_name DESC (latest first)
         - row_number() assigns 1, 2, 3... within each group
         - Filter row_number == 1 → keeps only the latest row per key

    4. Repartition to 8 files:
       After dedup, the data is in many small shuffle partitions.
       Repartition(8) creates 8 evenly-sized Parquet files.
       Rule of thumb: ~128 MB per file. ~1 GB total ÷ 128 MB = 8 files.
    """

    # Step 1: Derive Market_code from first 3 characters of Distributor_code
    # e.g. "INDDST01" → "IND",  "AUSDST01" → "AUS"
    df2 = df.withColumn(
        "Market_code",
        F.substring(F.col("Distributor_code"), 1, 3)  # start=1 (1-indexed), length=3
    )

    # Step 2: Cast all columns to Silver schema types using a list comprehension
    # For each field definition in SILVER_SCHEMA, create col(name).cast(type)
    df3 = df2.select(
        *[F.col(f.name).cast(f.dataType).alias(f.name) for f in SILVER_SCHEMA.fields]
    )

    # Step 3: Deduplication using a Window function
    # Window = "group these rows together and sort them this way"
    window_spec = (
        Window
        .partitionBy(*BUSINESS_KEY_COLS)   # group by all 6 business key columns
        .orderBy(
            F.col("load_timestamp").desc(),  # most recent load first
            F.col("file_name").desc()        # tiebreaker: alphabetically last filename
        )
    )

    df_deduped = (
        df3
        # row_number() gives 1 to the "winner" (latest) row in each group
        .withColumn("_rn", F.row_number().over(window_spec))
        # Keep only the winner (rank 1) — discard duplicates (rank 2, 3, ...)
        .filter(F.col("_rn") == 1)
        # Remove the helper column — not needed in the output
        .drop("_rn")
    )

    # Step 4: Repartition — merge many small shuffle partitions into 8 balanced files
    return df_deduped.repartition(8)


# ---------------------------------------------------------------------------
# WRITE TO SILVER: INITIAL LOAD OR INCREMENTAL MERGE
# ---------------------------------------------------------------------------
def upsert_to_silver(spark: SparkSession, df_deduped, silver_path: str, silver_table: str):
    """
    Writes deduplicated data to the Silver Delta Lake table.

    Two scenarios:
    ─────────────
    A) Table does NOT exist yet (first ever pipeline run):
       → Create the Delta table at silver_path and register it as silver_table.
       → Use overwrite mode + partitionBy date_reported.

    B) Table already exists (daily incremental run):
       → Run a SQL MERGE INTO statement (like a database upsert):
           - If a matching record exists AND incoming is newer → UPDATE
           - If no matching record exists → INSERT
           - If matching record exists but incoming is older → do nothing (MATCHED condition fails)
       This ensures the Silver table always has the most current data without
       growing duplicates even if the pipeline reruns for the same date.

    How MERGE works conceptually:
      Target (t) = existing Silver table
      Source  (s) = today's incoming deduplicated batch (registered as a temp SQL view)
      ON <business key match> = join condition to find matching rows

    merge_condition is built dynamically:
      "t.Market_code = s.Market_code AND t.Distributor_code = s.Distributor_code AND ..."
    """
    all_cols     = df_deduped.columns
    # Non-key columns = columns that are NOT part of the business key
    # These are the columns we UPDATE when a match is found
    non_key_cols = [c for c in all_cols if c not in BUSINESS_KEY_COLS]

    # Register the incoming batch as a temporary SQL view called "source_data"
    # This allows us to reference it in the MERGE SQL statement as: USING source_data s
    df_deduped.createOrReplaceTempView("source_data")

    # Build the SQL clauses dynamically from column lists
    # e.g. merge_condition = "t.Market_code = s.Market_code AND t.Distributor_code = s.Distributor_code ..."
    merge_condition = " AND ".join([f"t.{k} = s.{k}" for k in BUSINESS_KEY_COLS])

    # e.g. update_set_clause = "t.Brand = s.Brand,\n  t.Model = s.Model, ..."
    update_set_clause = ",\n          ".join([f"t.{c} = s.{c}" for c in non_key_cols])

    # e.g. "Market_code, Brand, Model, ..."
    insert_cols = ", ".join(all_cols)

    # e.g. "s.Market_code, s.Brand, s.Model, ..."
    insert_vals = ", ".join([f"s.{c}" for c in all_cols])

    # Check if the Silver Delta table already exists in the Spark catalog (metastore)
    table_exists = spark.catalog.tableExists(silver_table)

    if not table_exists:
        # ── SCENARIO A: FIRST RUN — create the Delta table ────────────────
        log.info("First run — creating Silver Delta table at: %s", silver_path)

        (
            df_deduped.write
            .format("delta")           # write as Delta Lake (not plain Parquet)
            .mode("overwrite")         # overwrite if something partial exists
            .option("path", silver_path)   # physical GCS location
            .partitionBy("date_reported")  # partition for faster date-range queries
            .saveAsTable(silver_table)     # register in Spark metastore for SQL access
        )
        log.info("Silver table created: %s | rows=%d", silver_path, df_deduped.count())

    else:
        # ── SCENARIO B: INCREMENTAL RUN — MERGE INTO existing table ───────
        log.info("Running incremental MERGE INTO: %s", silver_table)

        # SQL MERGE (Delta Lake feature — not available in plain Parquet)
        # t = target (existing Silver table)
        # s = source (today's incoming data, registered as "source_data" view above)
        spark.sql(f"""
            MERGE INTO {silver_table} t
            USING source_data s
            ON {merge_condition}
            WHEN MATCHED AND s.load_timestamp > t.load_timestamp THEN
              UPDATE SET {update_set_clause}
            WHEN NOT MATCHED THEN
              INSERT ({insert_cols}) VALUES ({insert_vals})
        """)
        # WHEN MATCHED AND newer → update the row (new price, stock, etc.)
        # WHEN MATCHED but older → do nothing (old data doesn't override new)
        # WHEN NOT MATCHED → insert as a new row (new market/store/product appeared)

        log.info("Silver MERGE completed.")


# ---------------------------------------------------------------------------
# MAIN — Entry Point
# ---------------------------------------------------------------------------
def main():
    args  = parse_args()
    spark = get_spark(args.env)
    log.info("Silver job starting | dt=%s | run_id=%s | env=%s | bucket=%s",
             args.dt, args.run_id, args.env, args.pipeline_bucket)

    # GCS path where Silver Delta files are stored
    silver_path  = f"gs://{args.pipeline_bucket}/silver/mobile_brands/silver_brands_ingest_delta"

    # Logical table name registered in Spark's metastore (used in SQL queries)
    # Format: <database>.<table>
    silver_table = "mobile_brands.silver_brands_ingest_delta"

    # Step 1: Read all brands' Bronze Parquet for today
    df = read_bronze(spark, args.pipeline_bucket, args.dt)

    # If no Bronze data found → abort the job (Airflow will mark it as failed)
    if df is None or df.rdd.isEmpty():
        log.error("No Bronze data for dt=%s — aborting Silver job.", args.dt)
        spark.stop()
        sys.exit(1)

    # Step 2: Apply transformations (Market_code derivation, schema, dedup, repartition)
    df_deduped = transform(df)

    # Step 3: Write to Silver (initial load or incremental merge)
    upsert_to_silver(spark, df_deduped, silver_path, silver_table)

    spark.stop()
    log.info("Silver job complete | dt=%s", args.dt)


if __name__ == "__main__":
    main()
