import os
import json
from typing import Any, Dict, Tuple

import joblib
import pandas as pd
from flask import Flask, jsonify, request

from Data_ingestion.data_ingestion import get_s3_client
from client import run_training_pipeline

app = Flask(__name__)


def _download_from_minio(bucket: str, key: str, local_path: str) -> None:
    s3 = get_s3_client()
    s3.download_file(bucket, key, local_path)


def load_artifacts() -> Tuple[Any, Dict[str, Any], Dict[str, Any]]:
    """
    Downloads model.pkl, threshold.json, schema.json from MinIO and returns them.
    """
    model_bucket = os.environ["MODEL_BUCKET"]
    model_key = os.environ["MODEL_KEY"]
    thresh_key = os.environ["THRESHOLD_KEY"]
    schema_key = os.environ.get("SCHEMA_KEY", "temperature/schema.json")

    _download_from_minio(model_bucket, model_key, "/tmp/model.pkl")
    _download_from_minio(model_bucket, thresh_key, "/tmp/threshold.json")
    _download_from_minio(model_bucket, schema_key, "/tmp/schema.json")

    model = joblib.load("/tmp/model.pkl")
    with open("/tmp/threshold.json", "r") as f:
        threshold_data = json.load(f)
    with open("/tmp/schema.json", "r") as f:
        schema = json.load(f)

    return model, threshold_data, schema


def build_row_from_payload(schema: Dict[str, Any], payload_features: Dict[str, Any]) -> pd.DataFrame:
    """
    Create a single-row DataFrame with exact feature_columns order.
    Missing fields are filled from schema defaults.
    Extra fields are ignored.
    """
    cols = schema["feature_columns"]
    defaults = schema["defaults"]
    categorical = set(schema.get("categorical_features", []))

    row: Dict[str, Any] = {}
    for c in cols:
        if c in payload_features:
            val = payload_features[c]
        else:
            val = defaults.get(c, 0)

        # light type casting
        if c in categorical:
            try:
                row[c] = int(val)
            except Exception:
                row[c] = int(defaults.get(c, 0))
        else:
            try:
                row[c] = float(val)
            except Exception:
                row[c] = float(defaults.get(c, 0.0))

    return pd.DataFrame([row], columns=cols)


@app.get("/check_health")
def check_health():
    return jsonify({"status": "ok"}), 200


@app.post("/train")
def train():
    try:
        rmse, threshold_data = run_training_pipeline()
    except Exception as e:
        return jsonify({"error": "Training failed", "detail": str(e)}), 500

    return jsonify(
        {
            "status": "trained",
            "rmse": float(rmse),
            "threshold_data": float(threshold_data["threshold"]),
            "rule": threshold_data.get("rule", "2x_rmse"),
        }
    ), 200


@app.post("/predict")
def predict():
    payload = request.get_json(silent=True) or {}
    features = payload.get("features", None)

    if not isinstance(features, dict):
        return jsonify({"error": "Payload must be JSON with a 'features' object"}), 400

    try:
        model, threshold_data, schema = load_artifacts()
    except Exception as e:
        return (
            jsonify(
                {
                    "error": "Model artifacts not found. Run POST /train first.",
                    "detail": str(e),
                }
            ),
            409,
        )

    X_row = build_row_from_payload(schema, features)
    pred = float(model.predict(X_row)[0])
    pred = max(min(pred, 40), -20)


    return jsonify(
        {
            "predicted_temperature_2m": pred,
            "horizon_hours": 3,
            "threshold": float(threshold_data["threshold"]),
            "rule": threshold_data.get("rule", "2x_rmse"),
        }
    ), 200


@app.get("/schema")
def schema():
    """Helpful endpoint to see what the model expects and what defaults will be used."""
    try:
        _, _, schema = load_artifacts()
    except Exception as e:
        return (
            jsonify({"error": "Schema not available. Run POST /train first.", "detail": str(e)}),
            409,
        )
    return jsonify(schema), 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "5000")), debug=True)
