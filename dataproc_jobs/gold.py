"""
dataproc_jobs/gold.py — Silver → Gold (aggregations + BigQuery write)
======================================================================
Reads all brands' Silver Parquet for the given dt, computes KPI
aggregations, and writes the Gold table to both GCS Parquet and BigQuery.

Idempotent: overwrites the BigQuery partition for the given dt.
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
log = logging.getLogger("gold")

BRANDS = ["Samsung", "Apple", "Oppo", "Vivo", "OnePlus"]


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--dt",              required=True)
    p.add_argument("--pipeline_bucket", required=True)
    p.add_argument("--project_id",      required=True)
    p.add_argument("--dataset",         default="mobile_brands")  # renamed from --bq_dataset
    p.add_argument("--env",             default="dv")
    args, _ = p.parse_known_args()  # ignore extra args passed by the DAG (e.g. --run_id, --source_bucket)
    return args


def get_spark(env: str) -> SparkSession:
    return (
        SparkSession.builder
        .appName(f"mb-gold-{env}")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )


def main():
    args  = parse_args()
    spark = get_spark(args.env)
    log.info("Gold job | dt=%s | env=%s", args.dt, args.env)

    # ── Load all silver data ──────────────────────────────────────────────────
    dfs = []
    for brand in BRANDS:
        path = f"gs://{args.pipeline_bucket}/silver/brand={brand}/dt={args.dt}"
        try:
            dfs.append(spark.read.parquet(path))
        except Exception as exc:
            log.warning("No silver for brand %s: %s", brand, exc)

    if not dfs:
        log.error("No silver data found for dt=%s — aborting.", args.dt)
        spark.stop()
        sys.exit(1)

    combined = dfs[0]
    for df in dfs[1:]:
        combined = combined.unionByName(df, allowMissingColumns=True)

    # ── KPI aggregations ──────────────────────────────────────────────────────
    gold = (
        combined
        .groupBy("brand", "dt")
        .agg(
            F.count("*").alias("total_records"),
            F.countDistinct("run_id").alias("pipeline_runs"),
            F.max("ingest_ts").alias("latest_ingest_ts"),
            F.max("silver_ts").alias("latest_silver_ts"),
        )
        .withColumn("gold_ts", F.lit(datetime.now(timezone.utc).isoformat()))
    )

    # ── Write to GCS Gold ─────────────────────────────────────────────────────
    gcs_dest = f"gs://{args.pipeline_bucket}/gold/dt={args.dt}"
    gold.write.mode("overwrite").parquet(gcs_dest)
    log.info("Gold GCS written: %s", gcs_dest)

    # ── Write to BigQuery ─────────────────────────────────────────────────────
    bq_table = f"{args.project_id}:{args.dataset}.gold_mobile_brands"
    (
        gold.write
        .format("bigquery")
        .option("table", bq_table)
        .option("partitionField", "dt")
        .option("writeMethod", "indirect")
        .option("temporaryGcsBucket", args.pipeline_bucket)
        .mode("overwrite")
        .save()
    )
    log.info("Gold BigQuery written: %s", bq_table)

    row_count = gold.count()
    spark.stop()
    log.info("Gold complete | rows=%d", row_count)


if __name__ == "__main__":
    main()
