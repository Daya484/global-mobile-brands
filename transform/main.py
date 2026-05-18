"""
transform/main.py — Cloud Run Job: Clean landing/ CSVs → transform/
====================================================================
Config loaded from config/config.json (overrideable by env vars).
Multi-threaded: each CSV blob processed in parallel.

GCS output (idempotent):
  transform/dt=YYYY-MM-DD/run_id=<run_id>/<folder>/<brand>.csv
"""

import io
import json
import logging
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from threading import Lock

import pandas as pd
from google.cloud import storage

# ── Config ────────────────────────────────────────────────────────────────────

def load_config() -> dict:
    cfg_path = Path(__file__).parent / "config" / "config.json"
    with open(cfg_path) as f:
        cfg = json.load(f)
    overrides = {
        "project_name":    os.getenv("PROJECT_NAME"),
        "pipeline_bucket": os.getenv("DEST_BUCKET"),
        "log_level":       os.getenv("LOG_LEVEL"),
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
    return logging.getLogger("transform")

# ── GCS helpers ───────────────────────────────────────────────────────────────

def get_gcs_client() -> storage.Client:
    return storage.Client()

def list_landing_blobs(client: storage.Client, bucket_name: str,
                       landing_path: str, dt: str, run_id: str) -> list:
    """List all CSV blobs in landing/dt=<dt>/run_id=<run_id>/"""
    prefix = f"{landing_path}/dt={dt}/run_id={run_id}/"
    return [b for b in client.list_blobs(bucket_name, prefix=prefix)
            if b.name.endswith(".csv") and b.size > 0]

def clean_dataframe(df: pd.DataFrame, brand: str, dt: str,
                    run_id: str, ingest_ts: str) -> pd.DataFrame:
    """Normalise headers, strip whitespace, add pipeline metadata columns."""
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].str.strip()
    df["brand"]     = brand
    df["dt"]        = dt
    df["run_id"]    = run_id
    df["ingest_ts"] = ingest_ts
    return df

def upload_transformed_csv(client: storage.Client, bucket_name: str,
                           df: pd.DataFrame, brand: str, folder: str,
                           transform_path: str, dt: str, run_id: str,
                           ingest_ts: str) -> str:
    """Write cleaned DataFrame as CSV to transform/ zone, return GCS URI."""
    dest_path = f"{transform_path}/dt={dt}/run_id={run_id}/{folder}/{brand}.csv"
    buf = io.BytesIO()
    df.to_csv(buf, index=False)
    buf.seek(0)
    blob = client.bucket(bucket_name).blob(dest_path)
    blob.metadata = {"brand": brand, "dt": dt, "run_id": run_id,
                     "ingest_ts": ingest_ts, "rows": str(len(df))}
    blob.upload_from_file(buf, content_type="text/csv")
    return f"gs://{bucket_name}/{dest_path}"

# ── Worker ────────────────────────────────────────────────────────────────────

_lock    = Lock()
_success = 0
_errors  = 0
log      = logging.getLogger("transform")

def _process_blob(client, cfg, blob, dt, run_id, ingest_ts) -> str:
    global _success, _errors
    try:
        raw    = blob.download_as_bytes()
        df     = pd.read_csv(io.BytesIO(raw))
        parts  = blob.name.split("/")
        brand  = parts[-1].replace(".csv", "")
        folder = parts[-2]
        df     = clean_dataframe(df, brand, dt, run_id, ingest_ts)
        path   = upload_transformed_csv(
            client, cfg["pipeline_bucket"], df, brand, folder,
            cfg["transform_path"], dt, run_id, ingest_ts,
        )
        log.info("Transformed → %s (%d rows)", path, len(df))
        with _lock:
            _success += 1
        return f"OK: {path}"
    except Exception as exc:
        log.error("FAILED: %s — %s", blob.name, exc, exc_info=True)
        with _lock:
            _errors += 1
        return f"FAILED: {blob.name} — {exc}"

# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    global log, _success, _errors
    _success = _errors = 0

    cfg = load_config()
    log = setup_logging(cfg.get("log_level", "INFO"))

    dt        = os.getenv("DT", datetime.now(timezone.utc).strftime("%Y-%m-%d"))
    run_id    = os.getenv("RUN_ID", f"manual__{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S')}")
    ingest_ts = datetime.now(timezone.utc).isoformat()

    log.info("Transform started | project=%s | bucket=%s | dt=%s | run_id=%s",
             cfg["project_name"], cfg["pipeline_bucket"], dt, run_id)

    client = get_gcs_client()
    blobs  = list_landing_blobs(client, cfg["pipeline_bucket"],
                                cfg["landing_path"], dt, run_id)

    if not blobs:
        log.warning("No landing files found for dt=%s run_id=%s — nothing to do.", dt, run_id)
        sys.exit(0)

    log.info("Found %d file(s) to transform.", len(blobs))

    with ThreadPoolExecutor(max_workers=cfg.get("max_workers", 8)) as executor:
        futures = [executor.submit(_process_blob, client, cfg, b, dt, run_id, ingest_ts)
                   for b in blobs]
        for future in as_completed(futures):
            result = future.result()
            if "FAILED" in result:
                log.error(result)

    log.info("Transform complete | success=%d | errors=%d", _success, _errors)
    if _errors > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
