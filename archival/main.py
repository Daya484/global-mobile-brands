"""
Cloud Run Job: Archive Files (Landing + Transformed)
=====================================================

Moves files from:
  1. landing/<COUNTRY>/file.xlsx
     → archive_landing/YYYY-MM-DD/<COUNTRY>/file.xlsx

  2. transformed/<brand>/file.csv
     → archive_transformed/YYYY-MM-DD/<brand>/file.csv

✅ Loads config from config/config.json
✅ ENV aware (dv/pd) → project_name = dv-env / pd-env
✅ Bucket defaults to {bucket_base}_{env} (or BUCKET_NAME override)
✅ Date filter checks only the filename, not the full GCS path
✅ Tracks errors per file; exits non-zero so Airflow stops the pipeline
✅ Supports Airflow RUN_DATE={{ ds }}
✅ Implements skip_if_destination_exists
"""

import json
import logging
import os
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import date
from typing import Optional, List, Tuple

from google.cloud import storage


# -----------------------------------------------------------------------------
# CONFIG LOADING
# -----------------------------------------------------------------------------

def load_config() -> dict:
    """Load config from config/config.json."""
    with open("config/config.json", "r") as f:
        return json.load(f)


def setup_logging(level: str) -> logging.Logger:
    """Logs go to stdout → Cloud Logging captures automatically in Cloud Run."""
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(message)s",
        stream=sys.stdout,
    )
    return logging.getLogger("archival")


# -----------------------------------------------------------------------------
# HELPERS
# -----------------------------------------------------------------------------

def list_files(
    bucket: storage.Bucket,
    prefix: str,
    extensions: Tuple[str, ...],
    run_date_yyyymmdd: Optional[str],
    log: logging.Logger,
) -> List[str]:
    """
    List blobs under <prefix>/ filtered by extension.
    If run_date_yyyymmdd is given, only include files whose *filename*
    (not the full GCS path) contains that date string.
    """
    files: List[str] = []

    for blob in bucket.list_blobs(prefix=f"{prefix}/"):
        if blob.name.endswith("/"):
            continue

        if not any(blob.name.lower().endswith(ext) for ext in extensions):
            continue

        if run_date_yyyymmdd:
            basename = blob.name.split("/")[-1]  # check filename only
            if run_date_yyyymmdd not in basename:
                continue

        files.append(blob.name)

    log.info("Found %d file(s) under gs://%s/%s/", len(files), bucket.name, prefix)
    return files


def move_file(
    bucket: storage.Bucket,
    src_path: str,
    dest_path: str,
    skip_if_destination_exists: bool,
    log: logging.Logger,
    error_counter: List[int],
) -> None:
    """
    Move a blob using copy + delete (GCS does not support native move).
    If skip_if_destination_exists=True and destination already exists,
    skip moving and do NOT delete source.
    """
    try:
        dest_blob = bucket.blob(dest_path)

        if skip_if_destination_exists and dest_blob.exists():
            log.info("Skip (destination exists): %s → %s", src_path, dest_path)
            return

        src_blob = bucket.blob(src_path)
        bucket.copy_blob(src_blob, bucket, dest_path)
        src_blob.delete()

        log.info("Moved: %s → %s", src_path, dest_path)

    except Exception as exc:
        log.error("Error moving %s: %s", src_path, exc)
        error_counter.append(1)


# -----------------------------------------------------------------------------
# LANDING ARCHIVE
# -----------------------------------------------------------------------------

def archive_landing(
    bucket: storage.Bucket,
    landing_prefix: str,
    archive_landing_root: str,
    archive_date: str,
    run_date_yyyymmdd: Optional[str],
    excel_exts: Tuple[str, ...],
    max_workers: int,
    skip_if_destination_exists: bool,
    log: logging.Logger,
    error_counter: List[int],
) -> int:
    """Move Excel files: landing/<COUNTRY>/file.xlsx → archive_landing/YYYY-MM-DD/<COUNTRY>/file.xlsx"""
    files = list_files(bucket, landing_prefix, excel_exts, run_date_yyyymmdd, log)

    def process(src: str) -> None:
        parts = src.split("/")
        if len(parts) < 3:
            log.warning("Skipping unexpected path (< 3 parts): %s", src)
            return

        country = parts[1]
        file_name = parts[-1]
        dest = f"{archive_landing_root}/{archive_date}/{country}/{file_name}"

        move_file(bucket, src, dest, skip_if_destination_exists, log, error_counter)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        list(executor.map(process, files))

    return len(files)


# -----------------------------------------------------------------------------
# TRANSFORMED ARCHIVE
# -----------------------------------------------------------------------------

def archive_transformed(
    bucket: storage.Bucket,
    transformed_prefix: str,
    archive_transformed_root: str,
    archive_date: str,
    run_date_yyyymmdd: Optional[str],
    csv_exts: Tuple[str, ...],
    max_workers: int,
    skip_if_destination_exists: bool,
    log: logging.Logger,
    error_counter: List[int],
) -> int:
    """Move CSV files: transformed/<brand>/file.csv → archive_transformed/YYYY-MM-DD/<brand>/file.csv"""
    files = list_files(bucket, transformed_prefix, csv_exts, run_date_yyyymmdd, log)

    def process(src: str) -> None:
        parts = src.split("/")
        if len(parts) < 3:
            log.warning("Skipping unexpected path (< 3 parts): %s", src)
            return

        brand = parts[1]
        file_name = parts[-1]
        dest = f"{archive_transformed_root}/{archive_date}/{brand}/{file_name}"

        move_file(bucket, src, dest, skip_if_destination_exists, log, error_counter)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        list(executor.map(process, files))

    return len(files)


# -----------------------------------------------------------------------------
# MAIN
# -----------------------------------------------------------------------------

def main() -> None:
    cfg = load_config()
    log = setup_logging(cfg.get("log_level", "INFO"))

    # ✅ ENV detection (dv/pd)
    env = os.getenv("ENV", "dv").strip().lower()

    # ✅ Your org standard project name
    project_name = f"{env}-env"

    # Allow Airflow to control date via RUN_DATE={{ ds }} (YYYY-MM-DD)
    run_date = os.getenv("RUN_DATE")
    if run_date:
        archive_date = run_date
        run_date_yyyymmdd = run_date.replace("-", "")
    else:
        archive_date = date.today().strftime("%Y-%m-%d")
        run_date_yyyymmdd = date.today().strftime("%Y%m%d")

    # ✅ Bucket: BUCKET_NAME overrides; else build {bucket_base}_{env}
    bucket_name = os.getenv("BUCKET_NAME")
    if not bucket_name:
        bucket_base = cfg["bucket_base"]
        bucket_name = f"{bucket_base}_{env}"

    landing_prefix = cfg.get("landing_prefix", "landing")
    transformed_prefix = cfg.get("transformed_root", "transformed")
    archive_landing_root = cfg.get("archive_landing_root", "archive_landing")
    archive_transformed_root = cfg.get("archive_transformed_root", "archive_transformed")

    excel_exts = tuple(cfg.get("excel_extensions", [".xlsx", ".xls", ".xlsm"]))
    csv_exts = tuple(cfg.get("csv_extensions", [".csv"]))
    max_workers = int(os.getenv("MAX_WORKERS", cfg.get("max_workers", 16)))

    skip_if_destination_exists = bool(cfg.get("skip_if_destination_exists", True))

    log.info("=" * 70)
    log.info("Archive Job | project=%s | env=%s | date=%s | bucket=%s",
             project_name, env, archive_date, bucket_name)
    log.info("skip_if_destination_exists=%s | max_workers=%s",
             skip_if_destination_exists, max_workers)
    log.info("=" * 70)

    # Initialise GCS client
    client = storage.Client()
    bucket = client.bucket(bucket_name)

    # Shared error counter
    error_counter: List[int] = []

    log.info("📦 Archiving LANDING files...")
    n_landing = archive_landing(
        bucket=bucket,
        landing_prefix=landing_prefix,
        archive_landing_root=archive_landing_root,
        archive_date=archive_date,
        run_date_yyyymmdd=run_date_yyyymmdd,
        excel_exts=excel_exts,
        max_workers=max_workers,
        skip_if_destination_exists=skip_if_destination_exists,
        log=log,
        error_counter=error_counter,
    )

    log.info("📦 Archiving TRANSFORMED files...")
    n_transformed = archive_transformed(
        bucket=bucket,
        transformed_prefix=transformed_prefix,
        archive_transformed_root=archive_transformed_root,
        archive_date=archive_date,
        run_date_yyyymmdd=run_date_yyyymmdd,
        csv_exts=csv_exts,
        max_workers=max_workers,
        skip_if_destination_exists=skip_if_destination_exists,
        log=log,
        error_counter=error_counter,
    )

    log.info("=" * 70)
    log.info(
        "Archive summary | landing_files=%d | transformed_files=%d | errors=%d",
        n_landing, n_transformed, len(error_counter),
    )
    log.info("=" * 70)

    if error_counter:
        log.error("Archive completed with %d error(s) — failing job.", len(error_counter))
        sys.exit(1)

    log.info("✅ Archive completed successfully.")


if __name__ == "__main__":
    main()