"""
archival/utils/generic.py — Shared utilities for the archival job
==================================================================
Provides:
  - load_config()       : load config/config.json with env-var overrides
  - get_gcs_client()    : returns google.cloud.storage.Client
  - setup_logging()     : configures structured logging
  - collect_blobs()     : find (src, dst) pairs to move for a given prefix
  - move_blob()         : copy blob to archive path then delete original
  - ensure_placeholder(): leave .keep file so source folder still appears
"""

import json
import logging
import os
import sys
from pathlib import Path

from google.cloud import storage

# ── Config loader ─────────────────────────────────────────────────────────────

_CONFIG_PATH = Path(__file__).parent.parent / "config" / "config.json"


def load_config() -> dict:
    with open(_CONFIG_PATH, "r") as f:
        cfg = json.load(f)

    env_overrides = {
        "project_name":    os.getenv("PROJECT_NAME"),
        "pipeline_bucket": os.getenv("DEST_BUCKET"),
        "log_level":       os.getenv("LOG_LEVEL"),
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
    return logging.getLogger("archival")


# ── GCS helpers ───────────────────────────────────────────────────────────────

def get_gcs_client() -> storage.Client:
    return storage.Client()


def collect_blobs(client: storage.Client, bucket_name: str,
                  archive_map: dict, dt: str, run_id: str) -> list[tuple]:
    """
    For each src_prefix → dst_prefix in archive_map, list all data blobs
    scoped to dt and run_id, and return [(src_name, dst_name)] pairs.
    """
    pairs: list[tuple] = []
    for src_prefix, dst_prefix in archive_map.items():
        blobs = client.list_blobs(bucket_name, prefix=src_prefix)
        for b in blobs:
            if b.name.endswith("/") or b.name.endswith(".keep") or b.size == 0:
                continue
            if dt and f"dt={dt}" not in b.name:
                continue
            if run_id and run_id != "manual" and f"run_id={run_id}" not in b.name:
                continue
            dst = b.name.replace(src_prefix, dst_prefix, 1)
            pairs.append((b.name, dst))
    return pairs


def ensure_placeholder(bucket: storage.Bucket, src_name: str):
    """Upload a .keep file in the source folder so it stays visible in GCS."""
    folder = "/".join(src_name.split("/")[:-1]) + "/.keep"
    ph = bucket.blob(folder)
    if not ph.exists():
        ph.upload_from_string(b"", content_type="application/octet-stream")


def move_blob(client: storage.Client, bucket: storage.Bucket,
              src_name: str, dst_name: str) -> str:
    """
    Copy blob to archive path, delete original, leave .keep placeholder.
    Returns 'OK: <path>' or 'FAILED: <path> — <error>'.
    """
    try:
        src_blob = bucket.blob(src_name)
        bucket.copy_blob(src_blob, bucket, dst_name)
        src_blob.delete()
        ensure_placeholder(bucket, src_name)
        return f"OK: {src_name}"
    except Exception as exc:
        return f"FAILED: {src_name} — {exc}"
