# Decision Tree: train on all data < today; hold out today
# HTTP entrypoint: train_dt_http

import os, io, json, logging, traceback
import numpy as np
import pandas as pd
from google.cloud import storage
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OneHotEncoder
from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer
from sklearn.tree import DecisionTreeRegressor
from sklearn.metrics import mean_absolute_error

# -------- ENV --------
PROJECT_ID    = os.getenv("PROJECT_ID", "")
GCS_BUCKET    = os.getenv("GCS_BUCKET", "")
DATA_KEY      = os.getenv("DATA_KEY", "structured/datasets/listings_master.csv")
OUTPUT_PREFIX = os.getenv("OUTPUT_PREFIX", "structured/predictions")
LOG_LEVEL     = os.getenv("LOG_LEVEL", "INFO")

logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s %(levelname)s %(message)s")

# -------- GCS helpers --------
def _read_csv_from_gcs(client: storage.Client, bucket: str, key: str) -> pd.DataFrame:
    b = client.bucket(bucket)
    blob = b.blob(key)
    if not blob.exists():
        raise FileNotFoundError(f"gs://{bucket}/{key} not found")
    return pd.read_csv(io.BytesIO(blob.download_as_bytes()))

def _write_csv_to_gcs(client: storage.Client, bucket: str, key: str, df: pd.DataFrame):
    b = client.bucket(bucket)
    blob = b.blob(key)
    blob.upload_from_string(df.to_csv(index=False), content_type="text/csv")

# -------- core logic --------
def run_once(dry_run: bool = False, max_depth: int = 12, min_samples_leaf: int = 10):
    client = storage.Client(project=PROJECT_ID)
    df = _read_csv_from_gcs(client, GCS_BUCKET, DATA_KEY)

    # required columns
    required = {"scraped_at", "price", "make", "model", "year", "mileage"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {sorted(missing)}")

    # Parse dates (UTC) and split
    df["scraped_at_dt"] = pd.to_datetime(df["scraped_at"], errors="coerce", utc=True)
    df["date"] = df["scraped_at_dt"].dt.date

    dates = sorted(d for d in df["date"].dropna().unique())
    if len(dates) < 2:
        return {"status": "noop", "reason": "need at least two distinct dates", "dates": [str(d) for d in dates]}

    # ✅ TODAY = max date; TRAIN = all rows with date < TODAY
    today = dates[-1]
    train_df = df[df["date"] < today].copy()
    holdout_df = df[df["date"] == today].copy()

    # Coerce numerics
    for c in ["price", "year", "mileage"]:
        if c in train_df.columns:
            train_df[c] = pd.to_numeric(train_df[c], errors="coerce")
        if c in holdout_df.columns:
            holdout_df[c] = pd.to_numeric(holdout_df[c], errors="coerce")

    # Drop rows without target in train
    train_df = train_df[train_df["price"].notna()]
    if len(train_df) < 40:
        return {"status": "noop", "reason": "too few training rows", "train_rows": int(len(train_df))}

    # Features / target
    target = "price"
    cat_cols = ["make", "model"]
    num_cols = ["year", "mileage"]
    feats = cat_cols + num_cols

    pre = ColumnTransformer(
        transformers=[
            ("num", SimpleImputer(strategy="median"), num_cols),
            ("cat", Pipeline([
                ("imp", SimpleImputer(strategy="most_frequent")),
                ("oh", OneHotEncoder(handle_unknown="ignore"))
            ]), cat_cols),
        ]
    )

    model = DecisionTreeRegressor(
        max_depth=max_depth,
        min_samples_leaf=min_samples_leaf,
        random_state=42
    )
    pipe = Pipeline([("pre", pre), ("model", model)])

    # Fit on ALL historical (everything before today)
    X_train = train_df[feats]
    y_train = train_df[target]
    pipe.fit(X_train, y_train)

    # Evaluate on today's holdout + produce predictions
    mae_today = None
    preds_df = pd.DataFrame()
    if not holdout_df.empty:
        X_h = holdout_df[feats]
        y_hat = pipe.predict(X_h)
        preds_df = holdout_df[["post_id", "scraped_at", "make", "model", "year", "mileage"]].copy()
        preds_df["pred_price"] = np.round(y_hat, 2)
        if holdout_df[target].notna().any():
            y_true = pd.to_numeric(holdout_df[target], errors="coerce")
            mask = y_true.notna()
            if mask.any():
                mae_today = float(mean_absolute_error(y_true[mask], y_hat[mask]))

    # Write predictions for today
    out_key = f"{OUTPUT_PREFIX}/preds_{today.strftime('%Y%m%d')}.csv"
    if not dry_run and len(preds_df) > 0:
        _write_csv_to_gcs(client, GCS_BUCKET, out_key, preds_df)
        logging.info("Wrote predictions to gs://%s/%s (%d rows)", GCS_BUCKET, out_key, len(preds_df))
    else:
        logging.info("Dry run or no holdout rows; skip write. Would write to gs://%s/%s", GCS_BUCKET, out_key)

    return {
        "status": "ok",
        "today": str(today),
        "train_rows": int(len(train_df)),
        "holdout_rows": int(len(holdout_df)),
        "mae_today": mae_today,
        "output_key": out_key,
        "dry_run": dry_run,
    }

# -------- HTTP entrypoint --------
def train_dt_http(request):
    try:
        body = request.get_json(silent=True) or {}
        result = run_once(
            dry_run=bool(body.get("dry_run", False)),
            max_depth=int(body.get("max_depth", 12)),
            min_samples_leaf=int(body.get("min_samples_leaf", 10)),
        )
        code = 200 if result.get("status") == "ok" else 204
        return (json.dumps(result), code, {"Content-Type": "application/json"})
    except Exception as e:
        logging.error("Error: %s", e)
        logging.error("Trace:\n%s", traceback.format_exc())
        return (json.dumps({"status": "error", "error": str(e)}), 500, {"Content-Type": "application/json"})
