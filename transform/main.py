import io
import os
import json
import logging
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
from google.cloud import storage


# -----------------------------------------------------------------------------
# 1) CONFIG LOADING
# -----------------------------------------------------------------------------

def load_config() -> dict:
    """
    Load config from config/config.json.
    Keep config separate so new members can change bucket/prefixes without code edits.
    """
    with open("config/config.json", "r") as f:
        return json.load(f)


def setup_logging(level: str) -> logging.Logger:
    """
    Logs go to stdout -> Cloud Logging captures automatically in Cloud Run.
    """
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] [%(threadName)s] %(message)s",
    )
    return logging.getLogger("transform")


# -----------------------------------------------------------------------------
# 2) THREAD-LOCAL GCS CLIENT (safe in multi-thread processing)
# -----------------------------------------------------------------------------

_thread_local = threading.local()

def get_client() -> storage.Client:
    """
    Each thread gets its own storage client (recommended for thread safety).
    """
    if not hasattr(_thread_local, "client"):
        _thread_local.client = storage.Client()
    return _thread_local.client


# -----------------------------------------------------------------------------
# 3) HELPERS
# -----------------------------------------------------------------------------

def list_excel_files(bucket: storage.Bucket, source_prefix: str, excel_exts: tuple,
                     run_date_yyyymmdd: str | None,
                     country_filter: str | None) -> dict[str, list[str]]:
    """
    Scan gs://bucket/<source_prefix>/ and return:
      {COUNTRY_FOLDER: [blob_name, ...]}

    Expected input structure (your screenshot):
      landing/AUSTRALIA/AUSDST01_20260509.xlsx
      landing/INDIA/INDDST01_20260509.xlsx
    """
    result: dict[str, list[str]] = {}

    blobs = bucket.list_blobs(prefix=f"{source_prefix}/")

    for blob in blobs:
        # skip folder marker
        if blob.name == f"{source_prefix}/":
            continue

        # only excel files
        if not blob.name.lower().endswith(excel_exts):
            continue

        parts = blob.name.split("/")
        # landing / COUNTRY / filename.xlsx
        if len(parts) < 3:
            continue

        country_folder = parts[1]

        # optional country filter
        if country_filter and country_folder.upper() != country_filter.upper():
            continue

        # optional date filter: filename contains _YYYYMMDD before extension
        if run_date_yyyymmdd:
            base = os.path.basename(blob.name)
            if f"_{run_date_yyyymmdd}" not in base:
                continue

        result.setdefault(country_folder, []).append(blob.name)

    return result


def download_blob(blob: storage.Blob) -> bytes:
    """
    Download file bytes into memory. Works well for typical Excel sizes.
    """
    buf = io.BytesIO()
    blob.download_to_file(buf)
    buf.seek(0)
    return buf.read()


def read_excel_sheets(file_bytes: bytes, blob_name: str, brand_sheets: list[str], log: logging.Logger) -> dict[str, pd.DataFrame]:
    """
    Read specified brand sheets from Excel and return {brand: dataframe}.
    If a sheet is missing, it is skipped.
    """
    sheets: dict[str, pd.DataFrame] = {}

    try:
        xls = pd.ExcelFile(io.BytesIO(file_bytes), engine="openpyxl")
    except Exception as e:
        log.error("Failed to parse Excel %s: %s", blob_name, e)
        return sheets

    for brand in brand_sheets:
        if brand not in xls.sheet_names:
            continue
        try:
            sheets[brand] = xls.parse(brand)
        except Exception as e:
            log.error("Failed sheet %s in %s: %s", brand, blob_name, e)

    return sheets


def df_to_csv_bytes(df: pd.DataFrame) -> bytes:
    """
    Convert dataframe to CSV bytes (UTF-8).
    """
    buf = io.StringIO()
    df.to_csv(buf, index=False)
    return buf.getvalue().encode("utf-8")


def excel_to_csv_name(filename: str, excel_exts: tuple) -> str:
    """
    Convert AUSDST01_20260509.xlsx -> AUSDST01_20260509.csv
    """
    for ext in excel_exts:
        if filename.lower().endswith(ext):
            return filename[:-len(ext)] + ".csv"
    return filename + ".csv"


def dest_path_for_brand(transformed_root: str, brand_folder: str, csv_name: str) -> str:
    """
    Output path (matches your screenshot):
      transformed/apple/AUSDST01_20260509.csv
    """
    return f"{transformed_root}/{brand_folder}/{csv_name}"


# -----------------------------------------------------------------------------
# 4) WORKER: process one Excel file
# -----------------------------------------------------------------------------

def process_file(blob_name: str,
                 bucket_name: str,
                 source_prefix: str,
                 transformed_root: str,
                 brand_sheets: list[str],
                 brand_folder_map: dict[str, str],
                 excel_exts: tuple,
                 skip_if_exists: bool,
                 log: logging.Logger):
    """
    1) Download Excel from landing/<country>/
    2) For each brand sheet -> create CSV
    3) Upload to transformed/<brand>/
    """
    client = get_client()
    bucket = client.bucket(bucket_name)

    src_blob = bucket.blob(blob_name)

    log.info("Processing %s", blob_name)

    try:
        file_bytes = download_blob(src_blob)
    except Exception as e:
        log.error("Download failed %s: %s", blob_name, e)
        return 0, 1

    sheets = read_excel_sheets(file_bytes, blob_name, brand_sheets, log)
    if not sheets:
        return 0, 0  # no sheets found, not necessarily error

    csv_name = excel_to_csv_name(os.path.basename(blob_name), excel_exts)

    uploaded = 0
    errors = 0

    for brand, df in sheets.items():
        brand_folder = brand_folder_map.get(brand)
        if not brand_folder:
            # If a brand is missing in map, skip safely
            log.warning("No folder mapping for brand=%s. Skipping.", brand)
            continue

        dest_path = dest_path_for_brand(transformed_root, brand_folder, csv_name)
        dest_blob = bucket.blob(dest_path)

        # Optional: skip re-processing if output already exists
        if skip_if_exists:
            try:
                if dest_blob.exists(client):
                    log.info("Skip (already exists): gs://%s/%s", bucket_name, dest_path)
                    continue
            except Exception:
                # If exists() fails, continue normal upload
                pass

        try:
            data = df_to_csv_bytes(df)
            dest_blob.upload_from_string(data, content_type="text/csv")
            log.info("✔ Uploaded gs://%s/%s", bucket_name, dest_path)
            uploaded += 1
        except Exception as e:
            log.error("Upload failed %s: %s", dest_path, e)
            errors += 1

    return uploaded, errors


# -----------------------------------------------------------------------------
# 5) MAIN
# -----------------------------------------------------------------------------

def main():
    cfg = load_config()
    log = setup_logging(cfg.get("log_level", "INFO"))

    bucket_name = os.getenv("BUCKET_NAME", cfg["bucket_name"])
    source_prefix = os.getenv("SOURCE_PREFIX", cfg["source_prefix"])
    transformed_root = os.getenv("TRANSFORMED_ROOT", cfg["transformed_root"])

    brand_sheets = cfg["brand_sheets"]
    brand_folder_map = cfg["brand_folder_map"]

    excel_exts = tuple(cfg.get("excel_extensions", [".xlsx", ".xls", ".xlsm"]))
    max_workers = int(os.getenv("MAX_WORKERS", cfg.get("max_workers", 10)))
    skip_if_exists = str(os.getenv("SKIP_IF_EXISTS", cfg.get("skip_if_transformed_exists", True))).lower() == "true"

    # Optional: Airflow passes RUN_DATE={{ ds }} => YYYY-MM-DD
    # We convert to YYYYMMDD to match filenames like AUSDST01_20260509.xlsx
    run_date = os.getenv("RUN_DATE")  # e.g., 2026-05-09
    run_date_yyyymmdd = run_date.replace("-", "") if run_date else None

    # Optional: process only one country folder (AUSTRALIA / INDIA / etc.)
    country_filter = os.getenv("COUNTRY_FILTER")  # e.g., AUSTRALIA

    client = storage.Client()
    bucket = client.bucket(bucket_name)

    log.info("=" * 80)
    log.info("Source: gs://%s/%s/<COUNTRY>/*.xlsx", bucket_name, source_prefix)
    log.info("Dest  : gs://%s/%s/<brand>/*.csv", bucket_name, transformed_root)
    if run_date_yyyymmdd:
        log.info("Filter: only files containing _%s", run_date_yyyymmdd)
    if country_filter:
        log.info("Filter: only country folder = %s", country_filter)
    log.info("=" * 80)

    folder_map = list_excel_files(bucket, source_prefix, excel_exts, run_date_yyyymmdd, country_filter)

    if not folder_map:
        log.warning("No Excel files found under gs://%s/%s/", bucket_name, source_prefix)
        return

    total_files = sum(len(v) for v in folder_map.values())
    log.info("Found %d file(s) across %d country folder(s).", total_files, len(folder_map))

    total_uploaded = 0
    total_errors = 0

    # We parallelize across all files (not only per country) for better throughput.
    # Thread-local client keeps GCS safe.
    all_files = []
    for _, blobs in folder_map.items():
        all_files.extend(blobs)

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = [
            pool.submit(
                process_file,
                blob_name,
                bucket_name,
                source_prefix,
                transformed_root,
                brand_sheets,
                brand_folder_map,
                excel_exts,
                skip_if_exists,
                log
            )
            for blob_name in all_files
        ]

        for f in as_completed(futures):
            uploaded, errors = f.result()
            total_uploaded += uploaded
            total_errors += errors

    log.info("=" * 80)
    log.info("DONE | input_files=%d | uploaded_csv=%d | errors=%d", total_files, total_uploaded, total_errors)
    log.info("=" * 80)

    # Fail the job if any file had upload errors so Airflow stops the pipeline
    if total_errors > 0:
        log.error("Transform completed with %d error(s) — failing job.", total_errors)
        raise SystemExit(1)


if __name__ == "__main__":
    main()