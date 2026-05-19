"""
dataproc_jobs/bronze.py — Raw CSV → Bronze Parquet
====================================================
Reads transform zone CSVs, appends metadata (ingest_ts, source_file,
run_id), and writes Parquet to bronze/ with partition dt=YYYY-MM-DD.

Idempotent: dynamic partition overwrite — safe to re-run.

Usage:
  spark-submit bronze.py \
    --dt=2024-01-15 \
    --run_id=scheduled__2024-01-15T010000 \
    --pipeline_bucket=dv-mb-pipeline-bucket \
    --env=dv
"""

import argparse
import logging
import sys
from datetime import datetime, timezone

from pyspark.sql import SparkSession, functions as F

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("bronze")

BRANDS = ["Samsung", "Apple", "Oppo", "Vivo", "OnePlus"]


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--dt",              required=True)
    p.add_argument("--run_id",          required=True)
    p.add_argument("--pipeline_bucket", required=True)
    p.add_argument("--source_bucket",   required=True)  # bucket holding transformed/ CSVs
    p.add_argument("--env",             default="dv")
    args, _ = p.parse_known_args()  # ignore extra args passed by the DAG
    return args


def get_spark(env: str) -> SparkSession:
    return (
        SparkSession.builder
        .appName(f"mb-bronze-{env}")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )


def data_quality_gate(df, brand: str):
    """Fail fast: row count > 0, mandatory cols not null."""
    row_count = df.count()
    if row_count == 0:
        raise RuntimeError(f"DQ FAIL: Zero rows for {brand}")
    for col in ["brand", "ingest_ts", "run_id", "dt"]:
        if col not in df.columns:
            raise RuntimeError(f"DQ FAIL: Missing column '{col}' in {brand}")
        if df.filter(F.col(col).isNull()).count() > 0:
            raise RuntimeError(f"DQ FAIL: Nulls in '{col}' for {brand}")
    log.info("DQ Gate passed for %s (%d rows)", brand, row_count)


def process_brand(spark, args, brand: str) -> int:
    # Transform writes: gs://<source_bucket>/transformed/<brand_lower>/<DISTCODE>_YYYYMMDD.csv
    date_nodash = args.dt.replace("-", "")
    src = f"gs://{args.source_bucket}/transformed/{brand.lower()}/*_{date_nodash}.csv"
    log.info("Reading: %s", src)

    try:
        df = spark.read.option("header", "true").csv(src)
    except Exception as exc:
        log.warning("No data for %s: %s", brand, exc)
        return 0

    if df.rdd.isEmpty():
        log.warning("Empty data for %s — skipping.", brand)
        return 0

    ingest_ts = datetime.now(timezone.utc).isoformat()
    df = (
        df
        .withColumn("brand",       F.lit(brand))
        .withColumn("ingest_ts",   F.lit(ingest_ts))
        .withColumn("source_file", F.lit(src))
        .withColumn("run_id",      F.lit(args.run_id))
        .withColumn("dt",          F.lit(args.dt))
    )

    # Data Quality Gate before writing
    data_quality_gate(df, brand)

    dest = f"gs://{args.pipeline_bucket}/bronze/brand={brand}/dt={args.dt}"
    count = df.count()
    df.write.mode("overwrite").parquet(dest)
    log.info("Bronze written: %s (%d rows)", dest, count)
    return count


def main():
    args  = parse_args()
    spark = get_spark(args.env)
    log.info("Bronze job | dt=%s | run_id=%s | env=%s", args.dt, args.run_id, args.env)

    total = 0
    failed = []
    for brand in BRANDS:
        try:
            total += process_brand(spark, args, brand)
        except RuntimeError as exc:
            log.error(exc)
            failed.append(brand)

    spark.stop()
    if failed:
        log.error("Bronze FAILED for: %s", failed)
        sys.exit(1)
    log.info("Bronze complete | total_rows=%d", total)


if __name__ == "__main__":
    main()
