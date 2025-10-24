# Minimal CF Gen2 HTTP entrypoint that writes a test file to GCS.
import os, json, datetime
from google.cloud import storage
from flask import Request, make_response

BUCKET_NAME = os.environ["BUCKET_NAME"]

def entrypoint(request: Request):
    ts = datetime.datetime.utcnow().isoformat() + "Z"
    blob_name = f"healthcheck/ok_{ts}.txt"

    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    bucket.blob(blob_name).upload_from_string(f"hello from CF at {ts}\n")

    return make_response(json.dumps({"ok": True, "blob": blob_name}), 200)
