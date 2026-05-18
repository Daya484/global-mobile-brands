"""
ingestion/utils/generic.py — Shared utilities for the ingestion job
====================================================================
Provides:
  - load_config()      : load config/config.json (overrideable by env vars)
  - get_gcs_client()   : returns google.cloud.storage.Client
  - setup_logging()    : configures structured logging
  - list_excel_blobs() : list all .xlsx blobs in a bucket
  - extract_brand_csv(): parse one workbook → {brand: csv_bytes}
"""

import io
import json
import logging
import os
import sys
from pathlib import Path

import openpyxl
from google.cloud import storage

# ── Config loader ─────────────────────────────────────────────────────────────

_CONFIG_PATH = Path(__file__).parent.parent / "config" / "config.json"


def load_config() -> dict:
    """
    Load config.json and override any key with a matching environment variable.
    e.g. SOURCE_BUCKET env var overrides config["source_bucket"]
    """
    with open(_CONFIG_PATH, "r") as f:
        cfg = json.load(f)

    # Allow runtime overrides via environment variables
    env_overrides = {
        "project_name":      os.getenv("PROJECT_NAME"),
        "source_bucket":     os.getenv("SOURCE_BUCKET"),
        "destination_bucket": os.getenv("DEST_BUCKET"),
        "log_level":         os.getenv("LOG_LEVEL"),
    }
    for key, val in env_overrides.items():
        if val is not None:
            cfg[key] = val

    return cfg


# ── Logging ───────────────────────────────────────────────────────────────────

def setup_logging(level: str = "INFO") -> logging.Logger:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
        stream=sys.stdout,
    )
    return logging.getLogger("ingestion")


# ── GCS helpers ───────────────────────────────────────────────────────────────

def get_gcs_client() -> storage.Client:
    return storage.Client()


def list_excel_blobs(client: storage.Client, bucket_name: str) -> list:
    """Return all non-empty .xlsx blobs from the given bucket."""
    blobs = client.list_blobs(bucket_name)
    return [b for b in blobs if b.name.lower().endswith(".xlsx") and b.size > 0]


def extract_brand_csv(blob, brands: list[str]) -> dict[str, bytes]:
    """
    Download an Excel blob and extract each brand sheet as CSV bytes.
    Returns {brand_name: csv_bytes} for sheets that exist and have data.
    """
    raw = blob.download_as_bytes()
    wb  = openpyxl.load_workbook(io.BytesIO(raw), read_only=True, data_only=True)

    result: dict[str, bytes] = {}
    for brand in brands:
        if brand not in wb.sheetnames:
            continue
        rows = list(wb[brand].values)
        if not rows:
            continue
        buf = io.StringIO()
        for row in rows:
            buf.write(",".join("" if v is None else str(v) for v in row) + "\n")
        result[brand] = buf.getvalue().encode("utf-8")

    wb.close()
    return result


def upload_csv(
    client: storage.Client,
    dest_bucket: str,
    brand: str,
    data: bytes,
    folder: str,
    dt: str,
    run_id: str,
    landing_path: str,
    ingest_ts: str,
) -> str:
    """Upload brand CSV to the idempotent landing path and return the GCS URI."""
    dest_path = f"{landing_path}/dt={dt}/run_id={run_id}/{folder}/{brand}.csv"
    blob      = client.bucket(dest_bucket).blob(dest_path)
    blob.metadata = {
        "ingest_ts":   ingest_ts,
        "source_file": folder,
        "run_id":      run_id,
        "brand":       brand,
    }
    blob.upload_from_string(data, content_type="text/csv")
    return f"gs://{dest_bucket}/{dest_path}"
