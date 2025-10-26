# main.py
# Build a single, ever-growing CSV from all structured JSONL files.
# Reads:  gs://<bucket>/<STRUCTURED_PREFIX>/run_id=*/jsonl/*.jsonl
# Writes: gs://<bucket>/<STRUCTURED_PREFIX>/datasets/listings_master.csv  (atomic publish)

import csv
import io
import json
import os
import re
from datetime import datetime, timezone
from typing import Dict, Iterable

from flask import Request, jsonify
from google.cloud import storage

# -------------------- ENV --------------------
BUCKET_NAME        = os.getenv("GCS_BUCKET")                      # REQUIRED
STRUCTURED_PREFIX  = os.getenv("STRUCTURED_PREFIX", "structured") # e.g., "structured"

storage_client = storage.Client()

# Accept run id format 20251026T170002Z only (that’s what extractor writes to structured/)
RUN_ID_RE = re.compile(r"^\d{8}T\d{6}Z$")

# Stable CSV schema for students
CSV_COLUMNS = [
    "post_id", "run_id", "scraped_at",
    "price", "year", "make", "model", "mileage",
    "source_txt"
]

def _list_run_ids(bucket: str, structured_prefix: str) -> list[str]:
    """Return sorted run_ids under gs://bucket/structured/run_id=*/"""
    it = storage_client.list_blobs(bucket, prefix=f"{structured_prefix}/", delimiter="/")
    for _ in it:  # populate it.prefixes
        pass
    run_ids = []
    for p in getattr(it, "prefixes", []):
        # p like 'structured/run_id=20251026T170002Z/'
        tail = p.rstrip("/").split("/")[-1]
        if tail.startswith("run_id="):
            rid = tail.split("run_id=", 1)[1]
            if RUN_ID_RE.match(rid):
                run_ids.append(rid)
    return sorted(run_ids)

def _jsonl_records_for_run(bucket: str, structured_prefix: str, run_id: str):
    """Yield dict records from .jsonl under .../run_id=<run_id>/jsonl/ (one JSON per file)."""
    b = storage_client.bucket(bucket)
    prefix = f"{structured_prefix}/run_id={run_id}/jsonl/"
    for blob in b.list_blobs(prefix=prefix):
        if not blob.name.endswith(".jsonl"):
            continue
        data = blob.download_as_text()
        line = data.strip()
        if not line:
            continue
        try:
            rec = json.loads(line)
            # ensure required keys exist
            rec.setdefault("run_id", run_id)
            yield rec
        except Exception:
            continue

def _run_id_to_dt(rid: str) -> datetime:
    return datetime.strptime(rid, "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc)

def _open_gcs_text_writer(bucket: str, key: str):
    """Open a streaming text writer to GCS."""
    b = storage_client.bucket(bucket)
    blob = b.blob(key)
    fh = blob.open("wb")  # resumable upload in binary
    return io.TextIOWrapper(fh, encoding="utf-8", write_through=True, newline="")

def _write_csv(records: Iterable[Dict], dest_key: str, columns=CSV_COLUMNS) -> int:
    out = _open_gcs_text_writer(BUCKET_NAME, dest_key)
    try:
        w = csv.DictWriter(out, fieldnames=columns, extrasaction="ignore")
        w.writeheader()
        n = 0
        for rec in records:
            row = {c: rec.get(c, None) for c in columns}
            w.writerow(row)
            n += 1
        return n
    finally:
        out.flush()
        out.detach().close()

def materialize_http(request: Request):
    """
    HTTP POST (no body needed).
    Crawls ALL structured run folders, de-dupes by post_id (keep newest run), and writes one CSV.
    Returns JSON with counts and output path.
    """
    if not BUCKET_NAME:
        return jsonify({"ok": False, "error": "missing GCS_BUCKET env"}), 500

    # Gather run_ids (sorted ascending so later runs overwrite earlier)
    run_ids = _list_run_ids(BUCKET_NAME, STRUCTURED_PREFIX)
    if not run_ids:
        return jsonify({"ok": False, "error": f"no runs found under {STRUCTURED_PREFIX}/"}), 200

    # Build “latest record per post_id”
    latest_by_post: Dict[str, Dict] = {}
    for rid in run_ids:
        for rec in _jsonl_records_for_run(BUCKET_NAME, STRUCTURED_PREFIX, rid):
            pid = rec.get("post_id")
            if not pid:
                continue
            # keep the newer run_id
            prev = latest_by_post.get(pid)
            if (prev is None) or (rec.get("run_id", "") > prev.get("run_id", "")):
                latest_by_post[pid] = rec

    # Stream to a temp object, then atomically publish to final path
    base = f"{STRUCTURED_PREFIX}/datasets"
    tmp_key = f"{base}/listings_master.csv.tmp"
    final_key = f"{base}/listings_master.csv"

    rows = _write_csv(latest_by_post.values(), tmp_key)

    # Atomic publish: copy tmp -> final, then delete tmp
    b = storage_client.bucket(BUCKET_NAME)
    src = b.blob(tmp_key)
    dst = b.blob(final_key)
    b.copy_blob(src, b, final_key)
    src.delete()

    return jsonify({
        "ok": True,
        "runs_scanned": len(run_ids),
        "unique_listings": len(latest_by_post),
        "rows_written": rows,
        "output_csv": f"gs://{BUCKET_NAME}/{final_key}"
    }), 200
