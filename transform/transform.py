"""
transform/transform.py — Multi-threaded Excel → CSV transformation
===================================================================
Reads CSVs from the landing zone, applies column-type cleaning,
standardises headers, and writes cleaned CSVs to the transform zone:

  transform/dt=YYYY-MM-DD/run_id=<run_id>/<folder>/<brand>.csv

Called by main.py.
"""

import io
import logging
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from threading import Lock

import pandas as pd
from google.cloud import storage

log = logging.getLogger("transform")

DEST_BUCKET  = os.environ["DEST_BUCKET"]
RUN_ID       = os.getenv("RUN_ID", "manual")
MAX_WORKERS  = int(os.getenv("TRANSFORM_WORKERS", "8"))

_lock          = Lock()
_success_count = 0
_error_count   = 0


def _list_landing_blobs(client: storage.Client, bucket_name: str, dt: str, run_id: str) -> list:
    prefix = f"landing/dt={dt}/run_id={run_id}/"
    blobs  = client.list_blobs(bucket_name, prefix=prefix)
    return [b for b in blobs if b.name.endswith(".csv") and b.size > 0]


def _clean_dataframe(df: pd.DataFrame, brand: str, dt: str, ingest_ts: str, run_id: str) -> pd.DataFrame:
    """Standardise headers, strip whitespace, add metadata columns."""
    # Normalise column names
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

    # Strip object columns
    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].str.strip()

    # Add idempotency metadata
    df["brand"]     = brand
    df["dt"]        = dt
    df["ingest_ts"] = ingest_ts
    df["run_id"]    = run_id
    return df


def _transform_single_blob(client: storage.Client, blob, dt: str, ingest_ts: str) -> str:
    global _success_count, _error_count

    try:
        raw     = blob.download_as_bytes()
        df      = pd.read_csv(io.BytesIO(raw))

        # Derive brand and folder from the blob path
        # landing/dt=.../run_id=.../<folder>/<brand>.csv
        parts   = blob.name.split("/")
        brand   = parts[-1].replace(".csv", "")
        folder  = parts[-2]

        df = _clean_dataframe(df, brand, dt, ingest_ts, RUN_ID)

        # Write cleaned CSV back to transform zone
        dest_path = f"transform/dt={dt}/run_id={RUN_ID}/{folder}/{brand}.csv"
        out_buf   = io.BytesIO()
        df.to_csv(out_buf, index=False)
        out_buf.seek(0)

        dest_bucket = client.bucket(DEST_BUCKET)
        dest_blob   = dest_bucket.blob(dest_path)
        dest_blob.metadata = {
            "brand":      brand,
            "dt":         dt,
            "run_id":     RUN_ID,
            "ingest_ts":  ingest_ts,
            "rows":       str(len(df)),
        }
        dest_blob.upload_from_file(out_buf, content_type="text/csv")

        log.info("Transformed → gs://%s/%s (%d rows)", DEST_BUCKET, dest_path, len(df))
        with _lock:
            _success_count += 1
        return f"OK: {dest_path}"

    except Exception as exc:
        log.error("FAILED: %s — %s", blob.name, exc, exc_info=True)
        with _lock:
            _error_count += 1
        return f"FAILED: {blob.name} — {exc}"


def run_transform(dt: str, run_id: str) -> dict:
    global _success_count, _error_count
    _success_count = _error_count = 0

    ingest_ts = datetime.now(timezone.utc).isoformat()
    client    = storage.Client()

    blobs = _list_landing_blobs(client, DEST_BUCKET, dt, run_id)
    if not blobs:
        log.warning("No landing files found for dt=%s run_id=%s", dt, run_id)
        return {"status": "empty", "success": 0, "errors": 0}

    log.info("Found %d file(s) to transform.", len(blobs))

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(_transform_single_blob, client, b, dt, ingest_ts) for b in blobs]
        for future in as_completed(futures):
            result = future.result()
            if "FAILED" in result:
                log.error(result)

    log.info("Transform complete | success=%d | errors=%d", _success_count, _error_count)
    return {
        "status":  "success" if _error_count == 0 else "partial",
        "success": _success_count,
        "errors":  _error_count,
    }
