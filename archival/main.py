"""
archival/main.py — Cloud Run Job: Archive landing/ + transform/ files
======================================================================
Config loaded from config/config.json (overrideable by env vars).
Moves both landing/ and transform/ files to archive/ in parallel.

Preserves source folder structure via .keep placeholder files.
"""

import json
import logging
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from threading import Lock

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
    return logging.getLogger("archival")

# ── GCS helpers ───────────────────────────────────────────────────────────────

def get_gcs_client() -> storage.Client:
    return storage.Client()

def collect_blobs(client: storage.Client, bucket_name: str,
                  archive_map: dict, dt: str, run_id: str) -> list[tuple]:
    """Return [(src_name, dst_name)] for all files to archive."""
    pairs = []
    for src_prefix, dst_prefix in archive_map.items():
        for b in client.list_blobs(bucket_name, prefix=src_prefix):
            if b.name.endswith("/") or b.name.endswith(".keep") or b.size == 0:
                continue
            if dt and f"dt={dt}" not in b.name:
                continue
            if run_id and run_id != "manual" and f"run_id={run_id}" not in b.name:
                continue
            pairs.append((b.name, b.name.replace(src_prefix, dst_prefix, 1)))
    return pairs

def ensure_placeholder(bucket: storage.Bucket, src_name: str):
    """Leave a .keep file so the source folder still appears in GCS."""
    folder = "/".join(src_name.split("/")[:-1]) + "/.keep"
    ph = bucket.blob(folder)
    if not ph.exists():
        ph.upload_from_string(b"", content_type="application/octet-stream")

def move_blob(client: storage.Client, bucket: storage.Bucket,
              src_name: str, dst_name: str) -> str:
    """Copy to archive path, delete original, leave .keep placeholder."""
    try:
        src_blob = bucket.blob(src_name)
        bucket.copy_blob(src_blob, bucket, dst_name)
        src_blob.delete()
        ensure_placeholder(bucket, src_name)
        return f"OK: {src_name}"
    except Exception as exc:
        return f"FAILED: {src_name} — {exc}"

# ── Entry point ───────────────────────────────────────────────────────────────

_lock    = Lock()
_success = 0
_failed  = 0
_errors  = []


def _move_with_tracking(client, bucket, src, dst) -> str:
    global _success, _failed
    result = move_blob(client, bucket, src, dst)
    with _lock:
        if result.startswith("OK"):
            _success += 1
        else:
            _failed += 1
            _errors.append(src)
    return result


def main():
    global _success, _failed
    _success = _failed = 0

    cfg = load_config()
    log = setup_logging(cfg.get("log_level", "INFO"))

    bucket_name = cfg["pipeline_bucket"]
    archive_map = cfg["archive_map"]
    dt          = os.getenv("DT", "")
    run_id      = os.getenv("RUN_ID", "manual")
    max_workers = cfg.get("max_workers", 8)

    log.info("Archival started | project=%s | bucket=%s | dt=%s | run_id=%s",
             cfg["project_name"], bucket_name, dt, run_id)

    client = get_gcs_client()
    bucket = client.bucket(bucket_name)
    pairs  = collect_blobs(client, bucket_name, archive_map, dt, run_id)

    if not pairs:
        log.info("No files to archive — nothing to do.")
        sys.exit(0)

    log.info("Archiving %d file(s) with %d workers...", len(pairs), max_workers)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(_move_with_tracking, client, bucket, src, dst)
                   for src, dst in pairs]
        for future in as_completed(futures):
            result = future.result()
            if "FAILED" in result:
                log.error(result)

    print("\n" + "=" * 60)
    print("              ARCHIVAL REPORT")
    print("=" * 60)
    print(f"Total Files: {len(pairs)}")
    print(f"Archived:    {_success}")
    print(f"Failed:      {_failed}")
    if _errors:
        print("\nFailed files:")
        for f in _errors:
            print(f"  - {f}")
    print(f"\nArchive path: gs://{bucket_name}/archive/")
    print("=" * 60)

    if _failed > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
