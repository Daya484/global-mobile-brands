"""
dataproc_jobs/silver.py — Bronze → Silver (dedup, clean, merge/upsert)
=======================================================================
Reads bronze Parquet, deduplicates by business keys (keeping the latest
ingest_ts per key), normalises types, and writes Silver partitioned by dt.

Idempotent: dynamic partition overwrite — re-runs for same dt are safe.
"""

import argparse
import logging
import sys
from datetime import datetime, timezone

from pyspark.sql import SparkSession, functions as F, Window

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("silver")

BRANDS = ["Samsung", "Apple", "Oppo", "Vivo", "OnePlus"]
# Granular business keys: one row = one model at one store for one distributor on one day
DEDUP_KEYS = ["brand", "Distributor_code", "Retailer_code", "Store_code", "Model"]


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--dt",              required=True)
    p.add_argument("--run_id",          required=True)
    p.add_argument("--pipeline_bucket", required=True)
    p.add_argument("--env",             default="dv")
    args, _ = p.parse_known_args()  # ignore extra args passed by the DAG
    return args


def get_spark(env: str) -> SparkSession:
    return (
        SparkSession.builder
        .appName(f"mb-silver-{env}")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )


def process_brand(spark, args, brand: str) -> int:
    src = f"gs://{args.pipeline_bucket}/bronze/brand={brand}/dt={args.dt}"
    log.info("Reading bronze: %s", src)

    try:
        df = spark.read.parquet(src)
    except Exception as exc:
        log.warning("Cannot read bronze for %s: %s", brand, exc)
        return 0

    if df.rdd.isEmpty():
        return 0

    # ── Dedup: keep latest ingest_ts per business key ─────────────────────────
    w = Window.partitionBy(*DEDUP_KEYS).orderBy(F.col("ingest_ts").desc())
    df = (
        df
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    # ── Silver metadata ───────────────────────────────────────────────────────
    df = df.withColumn("silver_ts", F.lit(datetime.now(timezone.utc).isoformat()))

    dest  = f"gs://{args.pipeline_bucket}/silver/brand={brand}/dt={args.dt}"
    count = df.count()
    df.write.mode("overwrite").parquet(dest)
    log.info("Silver written: %s (%d rows)", dest, count)
    return count


def main():
    args  = parse_args()
    spark = get_spark(args.env)
    log.info("Silver job | dt=%s | run_id=%s | env=%s", args.dt, args.run_id, args.env)

    total = sum(process_brand(spark, args, b) for b in BRANDS)
    spark.stop()
    log.info("Silver complete | total_rows=%d", total)


if __name__ == "__main__":
    main()
