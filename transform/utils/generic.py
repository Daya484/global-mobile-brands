"""
transform/utils/generic.py — Shared utilities for the transform job
====================================================================
Provides:
  - load_config()           : load config/config.json with env-var overrides
  - get_gcs_client()        : returns google.cloud.storage.Client
  - setup_logging()         : configures structured logging
  - list_landing_blobs()    : list CSVs in landing/dt=.../run_id=...
  - clean_dataframe()       : normalise headers, strip whitespace, add metadata
  - upload_transformed_csv(): write cleaned CSV to transform/ zone
"""

import io
import json
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from google.cloud import storage

# ── Config loader ─────────────────────────────────────────────────────────────

_CONFIG_PATH = Path(__file__).parent.parent / "config" / "config.json"


def load_config() -> dict:
    with open(_CONFIG_PATH, "r") as f:
        cfg = json.load(f)

    env_overrides = {
        "project_name":   os.getenv("PROJECT_NAME"),
        "pipeline_bucket": os.getenv("DEST_BUCKET"),
        "log_level":      os.getenv("LOG_LEVEL"),
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
    return logging.getLogger("transform")


# ── GCS helpers ───────────────────────────────────────────────────────────────

def get_gcs_client() -> storage.Client:
    return storage.Client()


def list_landing_blobs(client: storage.Client, bucket_name: str,
                       landing_path: str, dt: str, run_id: str) -> list:
    """List all CSV blobs in landing/dt=<dt>/run_id=<run_id>/"""
    prefix = f"{landing_path}/dt={dt}/run_id={run_id}/"
    blobs  = client.list_blobs(bucket_name, prefix=prefix)
    return [b for b in blobs if b.name.endswith(".csv") and b.size > 0]


def clean_dataframe(df: pd.DataFrame, brand: str, dt: str,
                    run_id: str, ingest_ts: str) -> pd.DataFrame:
    """
    Normalise column names, strip whitespace from string columns,
    and append pipeline metadata columns.
    """
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
    """Write cleaned DataFrame as CSV to transform/ zone and return GCS URI."""
    dest_path = f"{transform_path}/dt={dt}/run_id={run_id}/{folder}/{brand}.csv"
    buf = io.BytesIO()
    df.to_csv(buf, index=False)
    buf.seek(0)

    blob = client.bucket(bucket_name).blob(dest_path)
    blob.metadata = {
        "brand":     brand,
        "dt":        dt,
        "run_id":    run_id,
        "ingest_ts": ingest_ts,
        "rows":      str(len(df)),
    }
    blob.upload_from_file(buf, content_type="text/csv")
    return f"gs://{bucket_name}/{dest_path}"
