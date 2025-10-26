# extract.py
# Purpose: Convert TXT -> one-line JSON records (JSONL) AND one-row CSV per listing.
# Layout:
#   Input : gs://<bucket>/<SCRAPES_PREFIX>/run_id=<RUN_ID>/<post_id>.txt
#   Output: gs://<bucket>/<STRUCTURED_PREFIX>/run_id=<RUN_ID>/jsonl/<post_id>.jsonl
#           gs://<bucket>/<STRUCTURED_PREFIX>/run_id=<RUN_ID>/csv/<post_id>.csv
#
# Trigger: HTTP (use Cloud Scheduler at :05 past each hour)
# Body   : {"run_id":"20251024T200000Z","max_files":0,"overwrite":true}  # run_id optional

import os, re, csv, io, json, logging, traceback
from datetime import datetime, timezone
from flask import Request, jsonify
from google.cloud import storage
from google.api_core import retry as gax_retry

# -------- ENV --------
BUCKET_NAME       = os.getenv("GCS_BUCKET")
SCRAPES_PREFIX    = os.getenv("SCRAPES_PREFIX", "scrapes")
STRUCTURED_PREFIX = os.getenv("STRUCTURED_PREFIX", "structured")
RUN_ID_RE         = re.compile(r"^\d{8}T\d{6}Z$")

READ_RETRY = gax_retry.Retry(
    predicate=gax_retry.if_transient_error,
    initial=1.0, maximum=10.0, multiplier=2.0, deadline=120.0
)

storage_client = storage.Client()

# -------- SIMPLE PARSERS (lightweight, robust-ish) --------
PRICE_RE      = re.compile(r"\$\s?([0-9,]+)")
YEAR_RE       = re.compile(r"\b(19|20)\d{2}\b")
MAKE_MODEL_RE = re.compile(r"\b([A-Z][a-z]+)\s+([A-Z][A-Za-z0-9]+)")

def parse_fields(text: str) -> dict:
    out = {}

    # price_usd
    m = PRICE_RE.search(text)
    if m:
        try:
            out["price_usd"] = int(m.group(1).replace(",", ""))
        except ValueError:
            pass

    # year
    y = YEAR_RE.search(text)
    if y:
        try:
            out["year"] = int(y.group(0))
        except ValueError:
            pass

    # make/model (very naive; good enough for baseline)
    mm = MAKE_MODEL_RE.search(text)
    if mm:
        out["make"] = mm.group(1)
        out["model"] = mm.group(2)

    # mileage_mi (several patterns)
    mileage = None

    m1 = re.search(r"(?:mileage|odometer)\s*[:\-]?\s*([\d,]+)", text, re.I)
    if m1:
        try:
            mileage = int(m1.group(1).replace(",", ""))
        except ValueError:
            mileage = None

    if mileage is None:
        m2 = re.search(r"(\d+(?:\.\d+)?)\s*k\s*(?:mi|mile|miles)\b", text, re.I)
        if m2:
            try:
                mileage = int(float(m2.group(1)) * 1000)
            except ValueError:
                mileage = None

    if mileage is None:
        m3 = re.search(r"(\d{1,3}(?:[,\d]{3})*)\s*(?:mi|mile|miles)\b", text, re.I)
        if m3:
            try:
                mileage = int(re.sub(r"[^\d]", "", m3.group(1)))
            except ValueError:
                mileage = None

    if mileage is not None:
        out["mileage_mi"] = mileage

    return out

# -------- GCS HELPERS --------
def _list_latest_run_id(bucket: str) -> str | None:
    """Return newest run_id under gs://bucket/<SCRAPES_PREFIX>/run_id=YYYY...Z/"""
    it = storage_client.list_blobs(bucket, prefix=f"{SCRAPES_PREFIX}/", delimiter="/")
    # Drain iterator so prefixes populate
    for _ in it:
        pass
    candidates = []
    for p in getattr(it, "prefixes", []):
        # Expect "scrapes/run_id=YYYYMMDDTHHMMSSZ/"
        parts = p.strip("/").split("/")
        if len(parts) == 2 and parts[0] == SCRAPES_PREFIX and parts[1].startswith("run_id="):
            rid = parts[1].split("run_id=", 1)[1]
            if RUN_ID_RE.match(rid):
                candidates.append(rid)
    if not candidates:
        return None
    return sorted(candidates)[-1]

def list_txt_blob_names(bucket: str, run_id: str) -> list[str]:
    """List .txt under scrapes/run_id=<run_id>/"""
    prefix = f"{SCRAPES_PREFIX}/run_id={run_id}/"
    bkt = storage_client.bucket(bucket)
    return [b.name for b in bkt.list_blobs(prefix=prefix) if b.name.endswith(".txt")]

def download_text(bucket: str, blob_name: str) -> str:
    bkt = storage_client.bucket(bucket)
    return bkt.blob(blob_name).download_as_text(retry=READ_RETRY, timeout=120)

def upload_jsonl(bucket: str, blob_name: str, record: dict):
    bkt = storage_client.bucket(bucket)
    line = json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n"
    bkt.blob(blob_name).upload_from_string(line, content_type="application/x-ndjson")

def upload_csv_row(bucket: str, blob_name: str, header: list[str], row: list):
    bkt = storage_client.bucket(bucket)
    buf = io.StringIO(newline="")
    w = csv.writer(buf)
    w.writerow(header)
    w.writerow(row)
    bkt.blob(blob_name).upload_from_string(buf.getvalue(), content_type="text/csv")

def parse_run_id_iso(run_id: str) -> str:
    try:
        dt = datetime.strptime(run_id, "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc)
        return dt.isoformat().replace("+00:00", "Z")
    except Exception:
        return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

# -------- HTTP ENTRY --------
def extract_http(request: Request):
    """
    POST JSON:
      {
        "run_id": "20251024T200000Z",   # optional; if omitted the newest is used
        "max_files": 0,                 # optional; 0 = no cap
        "overwrite": true               # optional; default True for idempotency
      }
    """
    logging.getLogger().setLevel(logging.INFO)

    if not BUCKET_NAME:
        return jsonify({"ok": False, "error": "missing GCS_BUCKET env"}), 500

    try:
        body = request.get_json(silent=True) or {}
    except Exception:
        body = {}

    run_id    = body.get("run_id")
    max_files = int(body.get("max_files") or 0)
    overwrite = body.get("overwrite")
    if overwrite is None:
        overwrite = True  # default to overwrite per idempotent design

    # Resolve run_id
    if not run_id:
        run_id = _list_latest_run_id(BUCKET_NAME)
        if not run_id:
            return jsonify({"ok": False, "error": "no run_ids found under scrapes/"}), 200

    scraped_at_iso = parse_run_id_iso(run_id)
    txt_blobs = list_txt_blob_names(BUCKET_NAME, run_id)
    if max_files > 0:
        txt_blobs = txt_blobs[:max_files]

    processed = written_jsonl = written_csv = skipped = errors = 0

    # Output base prefixes
    out_base = f"{STRUCTURED_PREFIX}/run_id={run_id}"
    jsonl_base = f"{out_base}/jsonl"
    csv_base   = f"{out_base}/csv"

    bkt = storage_client.bucket(BUCKET_NAME)

    # Process each TXT
    for blob_name in txt_blobs:
        processed += 1
        try:
            # Derive post_id from the filename (e.g., ".../7888266471.txt" -> "7888266471")
            post_id = os.path.splitext(os.path.basename(blob_name))[0]

            # Skip existing if not overwriting
            jsonl_key = f"{jsonl_base}/{post_id}.jsonl"
            csv_key   = f"{csv_base}/{post_id}.csv"
            if not overwrite:
                if bkt.blob(jsonl_key).exists() and bkt.blob(csv_key).exists():
                    skipped += 1
                    continue

            text = download_text(BUCKET_NAME, blob_name)
            fields = parse_fields(text)

            # Build common record
            record = {
                "post_id": post_id,
                "run_id": run_id,
                "scraped_at": scraped_at_iso,
                **fields
            }

            # Write JSONL (single line)
            upload_jsonl(BUCKET_NAME, jsonl_key, record)
            written_jsonl += 1

            # Write CSV (single row + header)
            header = [
                "post_id", "run_id", "scraped_at",
                "price_usd", "year", "make", "model", "mileage_mi"
            ]
            row = [
                record.get("post_id", ""),
                record.get("run_id", ""),
                record.get("scraped_at", ""),
                record.get("price_usd", ""),
                record.get("year", ""),
                record.get("make", ""),
                record.get("model", ""),
                record.get("mileage_mi", "")
            ]
            upload_csv_row(BUCKET_NAME, csv_key, header, row)
            written_csv += 1

        except Exception as e:
            errors += 1
            logging.error(f"Failed {blob_name}: {e}\n{traceback.format_exc()}")

    result = {
        "ok": True,
        "run_id": run_id,
        "processed_txt": processed,
        "written_jsonl": written_jsonl,
        "written_csv": written_csv,
        "skipped_existing": skipped,
        "errors": errors
    }
    logging.info(json.dumps(result))
    return jsonify(result), 200
