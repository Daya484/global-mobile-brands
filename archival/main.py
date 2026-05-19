"""
Cloud Run Job: Archive Files (Landing + Transformed)
===================================================

This job moves files from:

1. landing/<COUNTRY>/file.xlsx
→ archive_landing/YYYY-MM-DD/<COUNTRY>/file.xlsx

2. transformed/<brand>/file.csv
→ archive_transformed/YYYY-MM-DD/<brand>/file.csv

✅ Works exactly like your GCS screenshot
✅ Safe: uses copy + delete (GCS move)
✅ Supports Airflow RUN_DATE
"""

import os
from datetime import date
from concurrent.futures import ThreadPoolExecutor
from google.cloud import storage


# -----------------------------------------------------------------------------
# CONFIG (ENV OR DEFAULTS)
# -----------------------------------------------------------------------------
BUCKET_NAME = os.getenv("BUCKET_NAME", "mobile_brands")

LANDING_PREFIX = "landing"
TRANSFORMED_PREFIX = "transformed"

ARCHIVE_LANDING = "archive_landing"
ARCHIVE_TRANSFORMED = "archive_transformed"

MAX_WORKERS = int(os.getenv("MAX_WORKERS", "10"))

# Airflow date
RUN_DATE = os.getenv("RUN_DATE")

if RUN_DATE:
    ARCHIVE_DATE = RUN_DATE
    RUN_DATE_YYYYMMDD = RUN_DATE.replace("-", "")
else:
    ARCHIVE_DATE = date.today().strftime("%Y-%m-%d")
    RUN_DATE_YYYYMMDD = date.today().strftime("%Y%m%d")


# -----------------------------------------------------------------------------
# GCS CLIENT
# -----------------------------------------------------------------------------

client = storage.Client()
bucket = client.bucket(BUCKET_NAME)


# -----------------------------------------------------------------------------
# HELPERS
# -----------------------------------------------------------------------------

def list_files(prefix, extensions):
    """
    List files in a prefix filtering by extension
    """
    files = []

    for blob in bucket.list_blobs(prefix=f"{prefix}/"):
        if blob.name.endswith("/"):
            continue
        if any(blob.name.lower().endswith(ext) for ext in extensions):

            # ✅ Only move today's files (important)
            if RUN_DATE_YYYYMMDD not in blob.name:
                continue

            files.append(blob.name)

    return files


def move_file(src_path, dest_path):
    """
    Move file using copy + delete
    """
    try:
        source_blob = bucket.blob(src_path)

        # Copy
        bucket.copy_blob(source_blob, bucket, dest_path)

        # Delete original
        source_blob.delete()

        print(f"✅ Moved: {src_path} → {dest_path}")

    except Exception as e:
        print(f"❌ Error moving {src_path}: {e}")


# -----------------------------------------------------------------------------
# LANDING ARCHIVE
# -----------------------------------------------------------------------------

def archive_landing():
    """
    Move Excel files from landing → archive_landing
    """
    files = list_files(LANDING_PREFIX, [".xlsx", ".xls"])

    def process(src):
        parts = src.split("/")
        if len(parts) < 3:
            return

        country = parts[1]
        file_name = parts[-1]

        dest = f"{ARCHIVE_LANDING}/{ARCHIVE_DATE}/{country}/{file_name}"

        move_file(src, dest)

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        executor.map(process, files)


# -----------------------------------------------------------------------------
# TRANSFORMED ARCHIVE
# -----------------------------------------------------------------------------

def archive_transformed():
    """
    Move CSV files from transformed → archive_transformed
    """
    files = list_files(TRANSFORMED_PREFIX, [".csv"])

    def process(src):
        parts = src.split("/")
        if len(parts) < 3:
            return

        brand = parts[1]
        file_name = parts[-1]

        dest = f"{ARCHIVE_TRANSFORMED}/{ARCHIVE_DATE}/{brand}/{file_name}"

        move_file(src, dest)

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        executor.map(process, files)


# -----------------------------------------------------------------------------
# MAIN
# -----------------------------------------------------------------------------

def main():
    print(f"\n🚀 Starting Archive Job for date: {ARCHIVE_DATE}\n")

    print("📦 Archiving LANDING files...")
    archive_landing()

    print("📦 Archiving TRANSFORMED files...")
    archive_transformed()

    print("\n✅ Archive Completed Successfully\n")


if __name__ == "__main__":
    main()