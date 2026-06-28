# Triggering redeploy to fix Cloud Run job issue
import io
import os
import json
import logging
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
from google.cloud import storage


# -----------------------------------------------------------------------------
# CONFIG LOADING
# -----------------------------------------------------------------------------

def load_config():
    with open("config/config.json", "r") as f:
        return json.load(f)


def setup_logging(level):
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] [%(threadName)s] %(message)s",
    )
    return logging.getLogger("transform")


# -----------------------------------------------------------------------------
# THREAD-LOCAL GCS CLIENT
# -----------------------------------------------------------------------------

_thread_local = threading.local()

def get_client():
    if not hasattr(_thread_local, "client"):
        _thread_local.client = storage.Client()
    return _thread_local.client


# -----------------------------------------------------------------------------
# HELPERS
# -----------------------------------------------------------------------------

def list_excel_files(bucket, source_prefix, excel_exts, run_date_yyyymmdd, country_filter):
    result = {}

    for blob in bucket.list_blobs(prefix=f"{source_prefix}/"):
        if blob.name.endswith("/"):
            continue

        if not blob.name.lower().endswith(excel_exts):
            continue

        parts = blob.name.split("/")
        if len(parts) < 3:
            continue

        country = parts[1]

        if country_filter and country.upper() != country_filter.upper():
            continue

        result.setdefault(country, []).append(blob.name)

    return result


def download_blob(blob):
    buf = io.BytesIO()
    blob.download_to_file(buf)
    buf.seek(0)
    return buf.read()


def read_excel_sheets(file_bytes, blob_name, brand_sheets, log):
    sheets = {}

    try:
        xls = pd.ExcelFile(io.BytesIO(file_bytes), engine="openpyxl")
    except Exception as e:
        log.error("Failed to read %s: %s", blob_name, e)
        return sheets

    for brand in brand_sheets:
        if brand not in xls.sheet_names:
            continue
        try:
            sheets[brand] = xls.parse(brand)
        except Exception as e:
            log.error("Error reading sheet %s in %s: %s", brand, blob_name, e)

    return sheets


def df_to_csv_bytes(df):
    buf = io.StringIO()
    df.to_csv(buf, index=False)
    return buf.getvalue().encode("utf-8")


def excel_to_csv_name(filename, excel_exts):
    for ext in excel_exts:
        if filename.lower().endswith(ext):
            return filename[:-len(ext)] + ".csv"
    return filename + ".csv"


def get_dest_path(transformed_root, brand_folder, csv_name):
    return f"{transformed_root}/{brand_folder}/{csv_name}"


# -----------------------------------------------------------------------------
# PROCESS FILE
# -----------------------------------------------------------------------------

def process_file(blob_name, bucket_name, transformed_root, brand_sheets,
                 brand_folder_map, excel_exts, skip_if_exists, log):

    client = get_client()
    bucket = client.bucket(bucket_name)

    src_blob = bucket.blob(blob_name)

    log.info("Processing %s", blob_name)

    try:
        data_bytes = download_blob(src_blob)
    except Exception as e:
        log.error("Download failed %s: %s", blob_name, e)
        return 0, 1

    sheets = read_excel_sheets(data_bytes, blob_name, brand_sheets, log)

    if not sheets:
        return 0, 0

    csv_name = excel_to_csv_name(os.path.basename(blob_name), excel_exts)

    uploaded = 0
    errors = 0

    for brand, df in sheets.items():
        folder = brand_folder_map.get(brand)
        if not folder:
            log.warning("Missing mapping for %s", brand)
            continue

        dest_path = get_dest_path(transformed_root, folder, csv_name)
        dest_blob = bucket.blob(dest_path)

        if skip_if_exists:
            try:
                if dest_blob.exists(client=client):
                    log.info("Skip exists: %s", dest_path)
                    continue
            except Exception:
                pass

        try:
            data = df_to_csv_bytes(df)
            dest_blob.upload_from_string(data, content_type="text/csv")
            uploaded += 1
        except Exception as e:
            log.error("Upload failed %s: %s", dest_path, e)
            errors += 1

    return uploaded, errors


# -----------------------------------------------------------------------------
# MAIN
# -----------------------------------------------------------------------------

def main():
    cfg = load_config()
    log = setup_logging(cfg.get("log_level", "INFO"))

    env = os.getenv("ENV", "dv")

    # ✅ Project name (your org format)
    project_name = f"{env}-env"

    # ✅ Bucket
    bucket_base = cfg.get("bucket_base", "mobile_brands")
    bucket_name = os.getenv("BUCKET_NAME", f"{bucket_base}_{env}")

    source_prefix = cfg.get("source_prefix", "landing")
    transformed_root = cfg.get("transformed_root", "transformed")

    brand_sheets = cfg["brand_sheets"]
    brand_folder_map = cfg["brand_folder_map"]

    excel_exts = tuple(cfg.get("excel_extensions", [".xlsx"]))
    max_workers = int(cfg.get("max_workers", 10))

    skip_if_exists = cfg.get("skip_if_transformed_exists", True)

    run_date = os.getenv("RUN_DATE")
    run_date_yyyymmdd = run_date.replace("-", "") if run_date else None

    country_filter = os.getenv("COUNTRY_FILTER")

    client = storage.Client()
    bucket = client.bucket(bucket_name)

    log.info("Project: %s | Bucket: %s", project_name, bucket_name)

    file_map = list_excel_files(
        bucket,
        source_prefix,
        excel_exts,
        run_date_yyyymmdd,
        country_filter
    )

    all_files = []
    for files in file_map.values():
        all_files.extend(files)

    total_uploaded = 0
    total_errors = 0

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(
                process_file,
                f,
                bucket_name,
                transformed_root,
                brand_sheets,
                brand_folder_map,
                excel_exts,
                skip_if_exists,
                log
            )
            for f in all_files
        ]

        for future in as_completed(futures):
            uploaded, errors = future.result()
            total_uploaded += uploaded
            total_errors += errors

    log.info("DONE | Uploaded=%d | Errors=%d", total_uploaded, total_errors)

    if total_errors > 0:
        raise SystemExit(1)


if __name__ == "__main__":
    main()