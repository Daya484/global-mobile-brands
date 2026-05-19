"""
Cloud Run Job: Archive Files (Landing + Transformed)
=====================================================

Moves files from:
  1. landing/<COUNTRY>/file.xlsx
     → archive_landing/YYYY-MM-DD/<COUNTRY>/file.xlsx

  2. transformed/<brand>/file.csv
     → archive_transformed/YYYY-MM-DD/<brand>/file.csv

✅ Loads config from config/config.json
✅ GCS client initialised inside main() — respects BUCKET_NAME env var
✅ Date filter checks only the filename, not the full GCS path
✅ Tracks errors per file; exits non-zero so Airflow stops the pipeline
✅ Supports Airflow RUN_DATE={{ ds }}
"""

import json
import logging
import os
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import date

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
    extensions: tuple,
    run_date_yyyymmdd: str | None,
    log: logging.Logger,
) -> list[str]:
    """
    List blobs under <prefix>/ filtered by extension.
    If run_date_yyyymmdd is given, only include files whose *filename*
    (not the full GCS path) contains that date string.
    """
    files = []
    for blob in bucket.list_blobs(prefix=f"{prefix}/"):
        if blob.name.endswith("/"):
            continue
        if not any(blob.name.lower().endswith(ext) for ext in extensions):
            continue
        if run_date_yyyymmdd:
            basename = blob.name.split("/")[-1]   # check filename only
            if run_date_yyyymmdd not in basename:
                continue
        files.append(blob.name)

    log.info("Found %d file(s) under gs://.../%s/", len(files), prefix)
    return files


def move_file(
    bucket: storage.Bucket,
    src_path: str,
    dest_path: str,
    log: logging.Logger,
    error_counter: list,
) -> None:
    """
    Move a blob using copy + delete (GCS does not support native move).
    Uses a shared list as a thread-safe error counter — list.append is GIL-safe.
    """
    try:
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
    run_date_yyyymmdd: str | None,
    excel_exts: tuple,
    max_workers: int,
    log: logging.Logger,
    error_counter: list,
) -> int:
    """Move Excel files: landing/<COUNTRY>/file.xlsx → archive_landing/YYYY-MM-DD/<COUNTRY>/file.xlsx"""
    files = list_files(bucket, landing_prefix, excel_exts, run_date_yyyymmdd, log)

    def process(src: str):
        parts = src.split("/")
        if len(parts) < 3:
            log.warning("Skipping unexpected path (< 3 parts): %s", src)
            return
        country   = parts[1]
        file_name = parts[-1]
        dest = f"{archive_landing_root}/{archive_date}/{country}/{file_name}"
        move_file(bucket, src, dest, log, error_counter)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        executor.map(process, files)

    return len(files)


# -----------------------------------------------------------------------------
# TRANSFORMED ARCHIVE
# -----------------------------------------------------------------------------

def archive_transformed(
    bucket: storage.Bucket,
    transformed_prefix: str,
    archive_transformed_root: str,
    archive_date: str,
    run_date_yyyymmdd: str | None,
    csv_exts: tuple,
    max_workers: int,
    log: logging.Logger,
    error_counter: list,
) -> int:
    """Move CSV files: transformed/<brand>/file.csv → archive_transformed/YYYY-MM-DD/<brand>/file.csv"""
    files = list_files(bucket, transformed_prefix, csv_exts, run_date_yyyymmdd, log)

    def process(src: str):
        parts = src.split("/")
        if len(parts) < 3:
            log.warning("Skipping unexpected path (< 3 parts): %s", src)
            return
        brand     = parts[1]
        file_name = parts[-1]
        dest = f"{archive_transformed_root}/{archive_date}/{brand}/{file_name}"
        move_file(bucket, src, dest, log, error_counter)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        executor.map(process, files)

    return len(files)


# -----------------------------------------------------------------------------
# MAIN
# -----------------------------------------------------------------------------

def main():
    cfg = load_config()
    log = setup_logging(cfg.get("log_level", "INFO"))

    # Allow Airflow to control "today" via RUN_DATE={{ ds }} (YYYY-MM-DD)
    run_date = os.getenv("RUN_DATE")
    if run_date:
        archive_date      = run_date
        run_date_yyyymmdd = run_date.replace("-", "")
    else:
        archive_date      = date.today().strftime("%Y-%m-%d")
        run_date_yyyymmdd = date.today().strftime("%Y%m%d")

    # Env var overrides config (enables different buckets per environment)
    bucket_name = os.getenv("BUCKET_NAME", cfg["bucket_name"])

    landing_prefix           = cfg.get("landing_prefix",           "landing")
    transformed_prefix       = cfg.get("transformed_root",          "transformed")
    archive_landing_root     = cfg.get("archive_landing_root",      "archive_landing")
    archive_transformed_root = cfg.get("archive_transformed_root",  "archive_transformed")

    excel_exts  = tuple(cfg.get("excel_extensions", [".xlsx", ".xls", ".xlsm"]))
    csv_exts    = tuple(cfg.get("csv_extensions",   [".csv"]))
    max_workers = int(os.getenv("MAX_WORKERS", cfg.get("max_workers", 16)))

    log.info("=" * 70)
    log.info("Archive Job | date=%s | bucket=%s", archive_date, bucket_name)
    log.info("=" * 70)

    # Initialise GCS client here (after env vars are read)
    client = storage.Client()
    bucket = client.bucket(bucket_name)

    # Shared error counter — list.append() is thread-safe under the GIL
    error_counter: list = []

    log.info("📦 Archiving LANDING files...")
    n_landing = archive_landing(
        bucket, landing_prefix, archive_landing_root,
        archive_date, run_date_yyyymmdd, excel_exts,
        max_workers, log, error_counter,
    )

    log.info("📦 Archiving TRANSFORMED files...")
    n_transformed = archive_transformed(
        bucket, transformed_prefix, archive_transformed_root,
        archive_date, run_date_yyyymmdd, csv_exts,
        max_workers, log, error_counter,
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