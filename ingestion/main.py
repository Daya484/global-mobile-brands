"""
ingestion/main.py — Cloud Run Job: Ingest Excel → landing/ CSVs
================================================================
Config loaded from config/config.json (overrideable by env vars).

GCS output (idempotent):
  landing/dt=YYYY-MM-DD/run_id=<run_id>/<folder>/<brand>.csv
"""

import io
import json
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import openpyxl
from google.cloud import storage

# ── Config ────────────────────────────────────────────────────────────────────

def load_config() -> dict:
    cfg_path = Path(__file__).parent / "config" / "config.json"
    with open(cfg_path) as f:
        cfg = json.load(f)
    # Allow env var overrides at runtime
    overrides = {
        "project_name":       os.getenv("PROJECT_NAME"),
        "source_bucket":      os.getenv("SOURCE_BUCKET"),
        "destination_bucket": os.getenv("DEST_BUCKET"),
        "log_level":          os.getenv("LOG_LEVEL"),
    }
    for k, v in overrides.items():
        if v is not None:
            cfg[k] = v
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
    """Return all non-empty .xlsx blobs from the source bucket."""
    return [
        b for b in client.list_blobs(bucket_name)
        if b.name.lower().endswith(".xlsx") and b.size > 0
    ]

def extract_brand_csv(blob, brands: list[str]) -> dict[str, bytes]:
    """Parse Excel blob → {brand: csv_bytes} for each brand sheet."""
    raw = blob.download_as_bytes()
    wb  = openpyxl.load_workbook(io.BytesIO(raw), read_only=True, data_only=True)
    result = {}
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

def upload_csv(client: storage.Client, dest_bucket: str, brand: str,
               data: bytes, folder: str, dt: str, run_id: str,
               landing_path: str, ingest_ts: str) -> str:
    """Upload brand CSV to idempotent landing path, return GCS URI."""
    dest_path = f"{landing_path}/dt={dt}/run_id={run_id}/{folder}/{brand}.csv"
    blob = client.bucket(dest_bucket).blob(dest_path)
    blob.metadata = {
        "ingest_ts": ingest_ts, "source_file": folder,
        "run_id": run_id, "brand": brand,
    }
    blob.upload_from_string(data, content_type="text/csv")
    return f"gs://{dest_bucket}/{dest_path}"

# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    cfg = load_config()
    log = setup_logging(cfg.get("log_level", "INFO"))

    source_bucket = cfg["source_bucket"]
    dest_bucket   = cfg["destination_bucket"]
    landing_path  = cfg["landing_path"]
    brands        = cfg["brands"]
    run_id        = os.getenv("RUN_ID", f"manual__{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S')}")
    now           = datetime.now(timezone.utc)
    dt            = now.strftime("%Y-%m-%d")
    ingest_ts     = now.isoformat()

    log.info("Ingestion started | project=%s | source=%s | dest=%s | run_id=%s | dt=%s",
             cfg["project_name"], source_bucket, dest_bucket, run_id, dt)

    client      = get_gcs_client()
    excel_blobs = list_excel_blobs(client, source_bucket)

    if not excel_blobs:
        log.warning("No Excel files found in gs://%s — nothing to do.", source_bucket)
        sys.exit(0)

    log.info("Found %d Excel file(s).", len(excel_blobs))
    uploaded = errors = 0

    for blob in excel_blobs:
        folder = blob.name.rsplit("/", 1)[0] if "/" in blob.name else "root"
        log.info("Processing: gs://%s/%s", source_bucket, blob.name)
        try:
            brand_data = extract_brand_csv(blob, brands)
        except Exception as exc:
            log.error("Failed to parse %s: %s", blob.name, exc, exc_info=True)
            errors += 1
            continue

        for brand, csv_bytes in brand_data.items():
            try:
                path = upload_csv(client, dest_bucket, brand, csv_bytes,
                                  folder, dt, run_id, landing_path, ingest_ts)
                log.info("Uploaded → %s", path)
                uploaded += 1
            except Exception as exc:
                log.error("Upload failed %s/%s: %s", blob.name, brand, exc, exc_info=True)
                errors += 1

    log.info("Ingestion complete | uploaded=%d | errors=%d", uploaded, errors)
    if errors > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
