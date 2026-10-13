import os
import json
from typing import Any, Dict, List, Tuple

import joblib
import pandas as pd

from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LinearRegression
from sklearn.metrics import root_mean_squared_error
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler

from Data_ingestion.data_ingestion import get_s3_client

TARGET_COL = "temperature_2m"


def load_historical() -> pd.DataFrame:
    """
    Downloads the historical CSV from MinIO to a local path and returns a DataFrame.
    """
    s3 = get_s3_client()

    bucket = os.environ["HISTORICAL_BUCKET"]
    key = os.environ["HISTORICAL_OBJECT"]
    local_path = os.environ.get("HISTORICAL_FILE", "/tmp/historical.csv")

    s3.download_file(bucket, key, local_path)
    return pd.read_csv(local_path)


def _add_time_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    If a 'date' column exists, create time-based features.
    """
    if "date" not in df.columns:
        return df

    df = df.copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce")

    df["year"] = df["date"].dt.year
    df["month"] = df["date"].dt.month
    df["day"] = df["date"].dt.day
    df["hour"] = df["date"].dt.hour
    df["weekday"] = df["date"].dt.weekday
    df["weekofyear"] = df["date"].dt.isocalendar().week.astype("int64")
    df["quarter"] = df["date"].dt.quarter
    return df


def _clean_numeric(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert columns to numeric when possible (except obvious non-numeric).
    """
    df = df.copy()

    # keep 'date' as is; we'll drop it later anyway
    for c in df.columns:
        if c == "date":
            continue
        # try numeric conversion; if it fails, it becomes NaN (OK -> imputer later)
        df[c] = pd.to_numeric(df[c], errors="coerce")

    # Ensure target is numeric
    if TARGET_COL in df.columns:
        df[TARGET_COL] = pd.to_numeric(df[TARGET_COL], errors="coerce")

    return df


def build_training_frame(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.Series]:
    """
    Returns X (features) and y (target). Ensures we never include target as a feature.
    """
    df = _add_time_features(df)
    df = _clean_numeric(df)

    if TARGET_COL not in df.columns:
        raise ValueError(f"Historical data must contain target column '{TARGET_COL}'")

    # Drop rows with missing target
    df = df.dropna(subset=[TARGET_COL]).copy()

    # Drop non-feature columns
    drop_cols = [TARGET_COL]
    if "date" in df.columns:
        drop_cols.append("date")
        X = df.drop(columns=[c for c in drop_cols if c in df.columns])
    for c in ["weather_code", "is_day"]:
     if c in X.columns:
        X[c] = pd.to_numeric(X[c], errors="coerce").fillna(0).astype(int)



    y = df[TARGET_COL].astype(float)

    return X, y


def compute_schema_defaults(X: pd.DataFrame, categorical_features: List[str]) -> Dict[str, Any]:
    """
    Compute per-feature defaults used by /predict when the user doesn't provide a field.
    Numeric -> median, categorical -> mode/most_frequent.
    """
    defaults: Dict[str, Any] = {}

    for c in X.columns:
        if c in categorical_features:
            # mode fallback
            try:
                mode_val = X[c].dropna().mode()
                defaults[c] = int(mode_val.iloc[0]) if len(mode_val) else 0
            except Exception:
                defaults[c] = 0
        else:
            # numeric median
            try:
                med = pd.to_numeric(X[c], errors="coerce").median()
                defaults[c] = float(med) if pd.notna(med) else 0.0
            except Exception:
                defaults[c] = 0.0

    return defaults


def train_and_build_artifacts() -> Tuple[Pipeline, float, Dict[str, Any], Dict[str, Any]]:
    """
    Trains a model pipeline and returns:
      - model pipeline
      - rmse
      - threshold_data
      - schema (feature_columns + defaults)
    """
    df = load_historical()
    df = df[(df["temperature_2m"] > -20) & (df["temperature_2m"] < 40)]

    X, y = build_training_frame(df)

    # Decide categorical columns (keep it simple)
    categorical_features: List[str] = []
    if "weather_code" in X.columns:
        categorical_features.append("weather_code")
    if "is_day" in X.columns:
        categorical_features.append("is_day")

    numeric_features = [c for c in X.columns if c not in categorical_features]

    numeric_transformer = Pipeline(
        steps=[
            ("imputer", SimpleImputer(strategy="median")),
            ("scaler", StandardScaler()),
        ]
    )

    categorical_transformer = Pipeline(
        steps=[
            ("imputer", SimpleImputer(strategy="most_frequent")),
            ("onehot", OneHotEncoder(handle_unknown="ignore")),
        ]
    )

    preprocessor = ColumnTransformer(
        transformers=[
            ("num", numeric_transformer, numeric_features),
            ("cat", categorical_transformer, categorical_features),
        ],
        remainder="drop",
    )

    model = Pipeline(
        steps=[
            ("preprocessor", preprocessor),
            ("regressor", LinearRegression()),
        ]
    )

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )

    model.fit(X_train, y_train)
    y_pred = model.predict(X_test)
    rmse = float(root_mean_squared_error(y_test, y_pred))

    threshold_data = {
        "rule": "2x_rmse",
        "threshold": 2.0 * rmse,
    }

    schema = {
        "target": TARGET_COL,
        "feature_columns": list(X.columns),
        "categorical_features": categorical_features,
        "defaults": compute_schema_defaults(X_train, categorical_features),
    }

    return model, rmse, threshold_data, schema


def save_and_upload_artifacts(model: Pipeline, threshold_data: Dict[str, Any], schema: Dict[str, Any]) -> None:
    """
    Saves model.pkl, threshold.json, schema.json locally and uploads to MinIO.
    """
    s3 = get_s3_client()

    model_bucket = os.environ["MODEL_BUCKET"]
    model_key = os.environ["MODEL_KEY"]
    thresh_key = os.environ["THRESHOLD_KEY"]
    schema_key = os.environ.get("SCHEMA_KEY", "temperature/schema.json")

    joblib.dump(model, "model.pkl")
    with open("threshold.json", "w") as f:
        json.dump(threshold_data, f)
    with open("schema.json", "w") as f:
        json.dump(schema, f)

    s3.upload_file("model.pkl", model_bucket, model_key)
    s3.upload_file("threshold.json", model_bucket, thresh_key)
    s3.upload_file("schema.json", model_bucket, schema_key)


def run_training_pipeline() -> Tuple[float, Dict[str, Any]]:
    """
    Trains model + uploads artifacts. Returns rmse and threshold_data.
    """
    model, rmse, threshold_data, schema = train_and_build_artifacts()
    save_and_upload_artifacts(model, threshold_data, schema)
    return rmse, threshold_data
