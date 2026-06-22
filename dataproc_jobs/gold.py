"""
dataproc_jobs/gold.py — Silver Delta → Gold (BigQuery nested schema)
=====================================================================
What this job does:
  1. Reads the unified Silver Delta Lake table (all brands, all dates)
  2. Loads 4 dimension tables from BigQuery (date, market, product, customer)
  3. Joins fact data with dimensions using a "salted join" to avoid data skew
  4. Selects a rich nested schema (STRUCTs) for the Gold layer
  5. Repartitions the result into 4 files
  6. Writes to BigQuery (overwrites the full table each run)

What is a "nested schema" (STRUCT)?
  Instead of flat columns, the Gold table has grouped columns inside STRUCTs:
  - GEOGRAPHY struct: { country_name, market_code2, market_code3, zone_code }
  - PRODUCT struct:   { product_code, brand, model, display, processor, ... }
  - FACT_VALUE struct:{ units_sold, sale_value, stock_units, stock_value, ... }
  This makes it easy to query related fields together in BigQuery.

What is a "salted join"?
  The calendar dimension has one row per date.
  If millions of fact rows share the same date, Spark sends all of them to
  ONE executor → that executor gets overloaded (data skew).
  Salting adds a random number (0-9) to both sides, spreading the work
  across 10 executors instead of 1. The rows still match correctly because
  both sides get the same random number before joining.

Usage (spark-submit on Dataproc):
  spark-submit gold.py \
    --pipeline_bucket=dv-mb-pipeline-bucket \
    --project_id=dev-env-496908 \
    --dataset=mobile_brands \
    --env=dv
"""

import argparse
import logging
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    broadcast,          # hint: send this small table to every executor (no shuffle)
    col,                # reference a column by name
    lit,                # create a column with a literal (constant) value
    current_timestamp,  # current datetime
    rand,               # random float between 0.0 and 1.0
    floor,              # round down to nearest integer (e.g. 7.8 → 7)
    regexp_replace,     # replace a pattern in a string column
    explode,            # expand one array row into multiple rows (one per element)
    array,              # create an array column from a list of values
    struct,             # combine multiple columns into one nested STRUCT column
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("gold")

# How many salt buckets to use for the calendar join.
# Higher = more parallelism but more shuffle. 10 is a good default.
SALT_BUCKETS = 10


# ---------------------------------------------------------------------------
# ARGUMENT PARSING
# ---------------------------------------------------------------------------
def parse_args():
    """
    Reads command-line arguments passed by Airflow via spark-submit.
    parse_known_args() silently ignores extra flags like --source_bucket
    that Airflow sends to all jobs but this script doesn't use.
    """
    p = argparse.ArgumentParser()
    p.add_argument("--pipeline_bucket", required=True,
                   help="GCS bucket with Silver Delta table; also used as BQ staging bucket")
    p.add_argument("--project_id",      required=True,
                   help="GCP project ID where BigQuery dataset lives (e.g. dv-env)")
    p.add_argument("--dataset",         default="mobile_brands",
                   help="BigQuery dataset name containing dim tables and the gold output table")
    p.add_argument("--env",             default="dv",
                   help="Environment: dv or prod")
    args, _ = p.parse_known_args()
    return args


# ---------------------------------------------------------------------------
# SPARK SESSION — with Delta Lake extensions
# ---------------------------------------------------------------------------
def get_spark(env: str) -> SparkSession:
    """
    Creates the Spark session.
    Delta Lake removed — reads Silver as Parquet (no extra JARs needed).
    shuffle.partitions=100 is better than the default 200 for this data volume.
    """
    return (
        SparkSession.builder
        .appName(f"mb-gold-{env}")
        .config("spark.sql.adaptive.enabled",      "true")
        .config("spark.sql.shuffle.partitions",    "100")
        .getOrCreate()
    )


# ---------------------------------------------------------------------------
# READ A BIGQUERY TABLE INTO A SPARK DATAFRAME
# ---------------------------------------------------------------------------
def read_bq_table(spark: SparkSession, project_id: str, dataset: str, table: str):
    """
    Reads a BigQuery table using the Spark BigQuery connector.
    The connector is pre-installed on Dataproc clusters.

    table format: project_id.dataset.table
    """
    return (
        spark.read
        .format("bigquery")                          # use the BigQuery connector
        .option("table", f"{project_id}.{dataset}.{table}")
        .load()                                      # returns a Spark DataFrame (lazy — not yet executed)
    )


# ---------------------------------------------------------------------------
# BUILD GOLD DATAFRAME — SALTED JOINS + NESTED SCHEMA
# ---------------------------------------------------------------------------
def build_gold(fact_df, dim_calender, dim_market, dim_product, dim_customer):
    """
    Joins the Silver fact table with 4 dimension tables and selects
    nested STRUCT columns for the Gold BigQuery schema.

    WHY SALTED JOIN for the calendar dimension?
    ───────────────────────────────────────────
    Normal join: millions of fact rows with date "2026-05-19"
    → all go to ONE executor holding the "2026-05-19" calendar row
    → that executor is overloaded (skew), others are idle

    Salted join:
      1. Add random int 0-9 to each fact row  → "2026-05-19" + salt=3
      2. Explode calendar → "2026-05-19" + salt=0, 1, 2, 3, ..., 9 (10 rows per date)
      3. Join on (date AND salt) → each unique combination goes to a different executor
      → workload spread evenly across 10 executors

    WHY broadcast() for market, product, customer?
    ──────────────────────────────────────────────
    These tables are small (< a few MB). broadcast() sends a full copy to every
    executor so Spark doesn't need a shuffle join (no network transfer per row).
    Much faster for small dimension tables.
    """

    # ── STEP 1: Prepare fact with salt and formatted date ─────────────────
    f_salted = (
        fact_df
        # Remove dashes from date: "2026-05-19" → "20260519" to match dim_calender.date_code
        .withColumn(
            "join_date",
            regexp_replace(col("date_reported").cast("string"), "-", "")
        )
        # Assign a random bucket number: floor(rand() * 10) gives integers 0-9
        .withColumn("salt", floor(rand() * SALT_BUCKETS))
        .alias("f")   # give this DataFrame the alias "f" so we can write f.column in joins
    )

    # ── STEP 2: Prepare calendar with exploded salt values ────────────────
    c_salted = (
        dim_calender
        # Keep date_code as string to match fact's join_date
        .withColumn("join_date",  col("date_code").cast("string"))
        # Create an array [0, 1, 2, ..., 9] — one row will become 10 rows
        # lit(i) creates a constant column with value i for each i in range(10)
        .withColumn("salt_array", array([lit(i) for i in range(SALT_BUCKETS)]))
        # explode() turns one row with [0,1,2,...,9] into 10 rows, one per value
        # e.g. date_code=20260519 → 10 rows: 20260519+salt=0, 20260519+salt=1, ...
        .select("*", explode(col("salt_array")).alias("salt"))
        .alias("c")
    )

    # ── STEP 3: Alias small dimensions for broadcast joins ─────────────────
    m      = dim_market.alias("m")      # small: ~200 country rows
    p      = dim_product.alias("p")     # small: product catalogue
    c_cust = dim_customer.alias("c")    # cached in parse: used in two places

    # ── STEP 4: Chain all joins together ──────────────────────────────────
    gold_joined = (
        f_salted
        # Join 1: Market — broadcast because dim_market is tiny (~200 rows)
        # Match: fact.Market_code == market.market_code3 (e.g. "IND")
        .join(broadcast(m),
              col("f.Market_code") == col("m.market_code3"),
              "left")   # left join = keep all fact rows, even if market not found

        # Join 2: Calendar (SALTED) — join on BOTH date AND salt to ensure 1-to-1
        # Without salt: every fact row for "20260519" goes to the same executor
        # With salt: rows spread across 10 executors based on random bucket
        .join(c_salted,
              (col("f.join_date") == col("c.join_date")) &
              (col("f.salt")      == col("c.salt")),
              "left")

        # Join 3: Product — broadcast because product catalogue is small
        # Match on BOTH Brand AND EAN_code to uniquely identify model/variant
        .join(broadcast(p),
              (col("f.Brand")    == col("p.brand")) &
              (col("f.EAN_code") == col("p.ean_code")),
              "left")

        # Join 4: Customer (store/retailer/distributor info) — broadcast
        # Match on Store_code to get store name, retailer, channel mode, etc.
        .join(broadcast(c_cust),
              col("f.Store_code") == col("c.store_code"),
              "left")
    )

    # ── STEP 5: Select nested Gold schema ─────────────────────────────────
    # struct() groups multiple columns into one nested STRUCT column.
    # In BigQuery, this becomes a RECORD type with sub-fields.
    # e.g. SELECT GEOGRAPHY.country_name, FACT_VALUE.units_sold FROM gold_table
    gold_df = gold_joined.select(

        # Flat top-level columns
        col("p.brand").alias("tech_brand_name"),
        lit(None).cast("string").alias("brand_segment"),  # not yet populated — placeholder
        col("m.market_name").alias("tech_orga_country_name"),

        # ── GEOGRAPHY STRUCT: where the sale happened ──────────────────────
        struct(
            col("m.market_name").alias("country_name"),   # full country name
            col("m.market_code2").alias("market_code2"),  # 2-letter code (e.g. "IN")
            col("m.market_code3").alias("market_code3"),  # 3-letter code (e.g. "IND")
            lit(None).cast("string").alias("zone_code"),  # future use
            lit(None).cast("string").alias("zone_name"),  # future use
        ).alias("GEOGRAPHY"),

        # ── TRANSACTION_DATE STRUCT: date hierarchy for reporting ──────────
        struct(
            col("f.date_reported").alias("period_date"),  # the actual date
            col("c.year").alias("year"),                  # 2026
            col("c.month").alias("month"),                # 5
            col("c.week_num").alias("week_num"),          # ISO week number
            col("c.month_year").alias("month_year"),      # "May-2026"
            col("c.quarter").alias("quarter"),            # "Q2"
            col("c.quarter_year").alias("quarter_year"),  # "Q2-2026"
        ).alias("TRANSACTION_DATE"),

        # ── PRODUCT STRUCT: what was sold ──────────────────────────────────
        struct(
            col("p.ean_code").alias("product_code"),              # barcode
            col("p.brand").alias("brand"),                        # "Samsung"
            col("p.model").alias("model"),                        # "Galaxy S24"
            col("p.display_specification").alias("display"),      # "6.2-inch AMOLED"
            col("p.processor_chipset").alias("processor"),        # "Snapdragon 8 Gen 3"
            col("p.front_camera").alias("rear_camera"),           # front camera spec
            col("p.back_camera").alias("back_camera"),            # back camera spec
            col("p.ram_gb").alias("ram"),                         # "8GB"
            col("p.rom_options").alias("rom"),                    # "128GB/256GB"
            col("p.refresh_rate_hz").alias("refresh_rate"),       # "120Hz"
            col("p.launch_year").alias("year_of_launch"),         # 2024
        ).alias("PRODUCT"),

        # ── CUSTOMER STRUCT: who sold it (channel/distribution chain) ──────
        struct(
            col("f.Distributor_code").alias("distributor_code"),   # "INDDST01"
            col("c.distributor_name").alias("distributor_name"),   # "ABC Distributors"
            col("f.Retailer_code").alias("retailer_code"),         # "INDRET01"
            col("c.retailer_name").alias("retailer_name"),         # "XYZ Electronics"
            col("f.Store_code").alias("store_code"),               # "INDSTR001"
            col("c.store_name").alias("store_name"),               # "XYZ Store - Delhi"
            col("c.channel_mode").alias("channel_mode"),           # "Modern Trade", "GT", etc.
        ).alias("CUSTOMER"),

        # ── CURRENCY STRUCT: pricing currency info ─────────────────────────
        struct(
            col("f.Currency").alias("currency"),                   # local currency "INR"
            col("m.hub_currency").alias("hub_currency"),           # hub reporting currency
            lit(None).cast("string").alias("conversion_rate"),     # future: FX rate
        ).alias("CURRENCY"),

        # ── FACT_VALUE STRUCT: the actual numbers ──────────────────────────
        struct(
            col("f.Price").alias("asp_local"),                               # average selling price
            col("f.Sale_units").alias("units_sold"),                         # units sold
            (col("f.Sale_units") * col("f.Price")).alias("sale_value"),      # revenue in local currency
            lit(None).cast("string").alias("total_sale_price_eur_value"),    # future: EUR conversion
            col("f.Stock_units").alias("stock_units"),                       # units in stock
            (col("f.Stock_units") * col("f.Price")).alias("stock_value"),    # stock value in local currency
            lit(None).cast("string").alias("total_stock_price_eur_value"),   # future: EUR conversion
        ).alias("FACT_VALUE"),

        # ── METADATA_TECHNICAL STRUCT: audit / lineage ─────────────────────
        struct(
            col("f.file_name").alias("file_name"),               # source file this row came from
            col("f.load_timestamp").alias("file_creation_date"), # when Bronze job processed it
            current_timestamp().alias("load_timestamp"),         # when Gold job processed it
        ).alias("METADATA_TECHNICAL"),
    )

    return gold_df


# ---------------------------------------------------------------------------
# MAIN — Entry Point
# ---------------------------------------------------------------------------
def main():
    args  = parse_args()
    spark = get_spark(args.env)
    log.info("Gold job starting | project=%s | dataset=%s | env=%s | bucket=%s",
             args.project_id, args.dataset, args.env, args.pipeline_bucket)

    # ── Paths and table names ─────────────────────────────────────────────
    # Silver Parquet table path on GCS (written by silver.py)
    silver_path = f"gs://{args.pipeline_bucket}/silver/mobile_brands/silver_brands_ingest"

    # Full BigQuery table reference
    gold_bq_table = f"{args.project_id}.{args.dataset}.gold_brand_daily_v1"

    # BigQuery indirect write staging bucket
    temp_gcs_bucket = args.pipeline_bucket

    # ── Step 1: Read Silver Parquet table ──────────────────────────────────
    log.info("Reading Silver Parquet: %s", silver_path)
    try:
        fact_df = spark.read.parquet(silver_path)
    except Exception as exc:
        log.error("Cannot read Silver Parquet table: %s", exc)
        spark.stop()
        sys.exit(1)

    # ── Step 2: Read BigQuery dimension tables ─────────────────────────────
    # These tables are maintained separately in BigQuery (not generated by this pipeline)
    log.info("Loading BigQuery dimensions: %s.%s.*", args.project_id, args.dataset)
    try:
        dim_calender = read_bq_table(spark,  args.project_id, args.dataset, "dim_date")
        dim_market   = read_bq_table(spark,  args.project_id, args.dataset, "dim_market")
        # .cache() keeps these DataFrames in memory after first use
        # so they don't get re-read from BQ if used in multiple joins
        dim_product  = read_bq_table(spark,  args.project_id, args.dataset, "product_v2").cache()
        dim_customer = read_bq_table(spark,  args.project_id, args.dataset, "customer").cache()
        log.info("BigQuery dimensions loaded successfully.")
    except Exception as exc:
        log.error("Failed to load BigQuery dimensions: %s", exc)
        spark.stop()
        sys.exit(1)

    # ── Step 3: Build Gold DataFrame (joins + nested schema) ──────────────
    gold_df = build_gold(fact_df, dim_calender, dim_market, dim_product, dim_customer)

    # Repartition to 4 files — data is ~150 MB so 4 × 37 MB files is optimal
    # Without repartition, we'd get many tiny files (1 per shuffle partition)
    gold_df = gold_df.repartition(4)

    # ── Step 4: Write to BigQuery ─────────────────────────────────────────
    log.info("Writing Gold to BigQuery: %s", gold_bq_table)
    try:
        # count() triggers the entire DAG of computations (joins, schema, repartition)
        # We do this before the write to log the row count first
        row_count = gold_df.count()
        log.info("Writing %d rows to BigQuery...", row_count)

        (
            gold_df.write
            .format("bigquery")               # use the BigQuery Spark connector
            .option("table",        gold_bq_table)
            .option("writeMethod", "indirect")  # stage to GCS first, then load to BQ
                                                # (more reliable than direct write for large data)
            .option("temporaryGcsBucket", temp_gcs_bucket)  # staging area for indirect write
            .mode("overwrite")                # overwrite the full BQ table each run
                                              # (Gold is a full refresh — not incremental)
            .save()
        )
        log.info("Gold written to BigQuery: %s | rows=%d", gold_bq_table, row_count)

    except Exception as exc:
        log.error("Failed to write Gold to BigQuery: %s", exc)
        spark.stop()
        sys.exit(1)   # sys.exit(1) → tells Airflow this task FAILED

    spark.stop()
    log.info("Gold job complete.")


if __name__ == "__main__":
    main()
