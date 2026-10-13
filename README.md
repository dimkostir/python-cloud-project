# Weather Lambda Pipeline — Cloud-Native Temperature Forecasting & Alerting

A containerized, cloud-style data pipeline that ingests live weather data from the **Open-Meteo API**, stores it in **MinIO** object storage, trains a **Linear Regression** model to forecast air temperature, serves predictions through a **Flask REST API**, and raises **real-time deviation alerts** through **RabbitMQ**.

The whole stack runs with a single `docker compose up`.

> **Scope note:** The focus of this project is the cloud / Lambda-style architecture (storage, messaging, serving, containerization). The ML model is intentionally simple and is used to demonstrate the end-to-end flow — it is not meant to be a production-grade weather forecaster.

---

## Table of Contents

1. [Features](#features)
2. [Tech Stack](#tech-stack)
3. [Quick Start](#quick-start)
4. [Architecture](#architecture)
5. [Components](#components)
6. [Data Flow](#data-flow)
7. [Storage Layout (MinIO)](#storage-layout-minio)
8. [REST API](#rest-api)
9. [Messaging & Alerting (RabbitMQ)](#messaging--alerting-rabbitmq)
10. [Machine Learning Pipeline](#machine-learning-pipeline)
11. [Configuration](#configuration)
12. [Project Structure](#project-structure)
13. [Limitations & Future Work](#limitations--future-work)

---

## Features

- Pulls current and recent hourly weather observations from Open-Meteo (default location: Patras, Greece)
- Stores raw, processed and model data in separate S3-compatible buckets
- Incrementally builds a historical training dataset
- Trains a scikit-learn pipeline (imputation → scaling → one-hot encoding → Linear Regression)
- Exposes `/train`, `/predict`, `/schema` and `/check_health` endpoints
- Consumes observations from a queue, compares actual vs. predicted temperature and publishes an **alert** when the error exceeds `2 × RMSE`
- Fully containerized with Docker Compose

---

## Tech Stack

| Layer            | Technology                          |
|------------------|-------------------------------------|
| Data source      | Open-Meteo API                      |
| Object storage   | MinIO (S3-compatible) via `boto3`   |
| Message broker   | RabbitMQ (`pika`)                   |
| Serving layer    | Flask                               |
| ML               | scikit-learn, pandas, joblib        |
| Orchestration    | Docker / Docker Compose             |
| Language         | Python 3                            |

---

## Quick Start

### Prerequisites

- Docker & Docker Compose
- `curl` (for testing the API)

### 1. Start the stack

```bash
docker compose up -d
```

### 2. Open the dashboards

| Service              | URL                        |
|----------------------|----------------------------|
| MinIO console        | http://localhost:9001      |
| RabbitMQ management  | http://localhost:15672     |
| Flask API            | http://localhost:5001      |

Credentials are set through environment variables in `docker-compose.yml` (see [Configuration](#configuration)).

### 3. Check that the API is alive

```bash
curl http://localhost:5001/check_health
# {"status": "ok"}
```

### 4. Train the model

```bash
curl -X POST http://localhost:5001/train
```

```json
{
  "status": "trained",
  "rmse": 0.146,
  "threshold_data": 0.293,
  "rule": "2x_rmse"
}
```

### 5. Get a prediction

```bash
curl -X POST http://localhost:5001/predict \
  -H "Content-Type: application/json" \
  -d '{"features": {"hour": 16, "weather_code": 2}}'
```

```json
{
  "predicted_temperature_2m": 23.19,
  "horizon_hours": 3,
  "threshold": 0.293,
  "rule": "2x_rmse"
}
```

Any feature not included in the payload is filled with a default value (see [`/schema`](#get-schema)).

---

## Architecture

```
                ┌──────────────────┐
                │  Open-Meteo API  │
                └────────┬─────────┘
                         │ (1) fetch hourly data
                         ▼
              ┌─────────────────────┐
              │  data_ingestion.py  │
              └────────┬────────────┘
                       │ (2) raw JSON snapshot
                       ▼
 ┌────────────────────────── MinIO ───────────────────────────┐
 │  raw-weather/     processed-weather/       models/          │
 │  obs_*.json   ─▶  history/historical.csv ─▶ temperature/   │
 │       (3) processor.py      (4) client.py   model.pkl       │
 │                                              threshold.json │
 │                                              schema.json    │
 └──────────────────────────────────────────────┬──────────────┘
                                                │ (5) load artifacts
                                                ▼
                                     ┌─────────────────────┐
             curl / clients ───────▶ │  Flask API (main.py)│
                                     │ /train /predict ... │
                                     └──────────▲──────────┘
                                                │ (7) POST /predict
 ┌──────────────── RabbitMQ ────────────────┐   │
 │ weather.observations ──(6)──▶ rabbit.py ─┼───┘
 │                                │         │
 │ alerts.temp_deviation ◀──(8)───┘         │
 │   (only if |actual − predicted| > 2·RMSE)│
 └──────────────────────────────────────────┘
```

The design follows **Lambda Architecture** ideas:

- **Batch layer** — raw data is accumulated in object storage, consolidated into a historical dataset and used to (re)train the model.
- **Speed layer** — individual observations flow through RabbitMQ and are evaluated against the model in near real time.
- **Serving layer** — the Flask API exposes the trained model and its metadata.

---

## Components

| File                 | Role |
|----------------------|------|
| `data_ingestion.py`  | Bridge to MinIO. Provides `get_s3_client()` (builds a `boto3` client from env vars) and uploads raw Open-Meteo snapshots to the `raw-weather` bucket. Shared by all other services. |
| `processor.py`       | Reads raw JSON snapshots, flattens the hourly data into rows (keeping one row every 3 hours), de-duplicates by date and **upserts** them into `historical.csv`. |
| `client.py`          | Training module. Loads `historical.csv`, builds features/target, trains the scikit-learn pipeline, computes RMSE and the alert threshold, generates the schema and uploads all artifacts to MinIO. |
| `main.py`            | Flask serving layer. Exposes the REST endpoints and loads artifacts from MinIO on prediction. |
| `rabbit.py`          | RabbitMQ consumer. Reads observations, calls `/predict`, computes the absolute error and publishes alerts. |
| `docker-compose.yml` | Defines all services, ports, volumes and environment variables. |

---

## Data Flow

1. **Ingest** — `data_ingestion.py` calls Open-Meteo for the configured coordinates and the last `PAST_HOURS` hours, then stores the response as `raw-weather/YYYY/MM/DD/obs_<timestamp>.json`.
2. **Process** — `processor.py` converts every raw snapshot into tabular rows and merges them into `processed-weather/history/historical.csv` (sorted by date, duplicates removed, latest value kept).
3. **Train** — `POST /train` runs the training pipeline and writes `model.pkl`, `threshold.json` and `schema.json` to the `models` bucket.
4. **Serve** — `POST /predict` loads the artifacts, builds a single-row DataFrame in the correct column order, fills missing features with defaults and returns the prediction.
5. **Monitor** — an observation published to `weather.observations` is consumed by `rabbit.py`, which compares the actual temperature with the prediction and, if needed, publishes an alert to `alerts.temp_deviation`.

---

## Storage Layout (MinIO)

```
raw-weather/
└── YYYY/MM/DD/
    └── obs_YYYYMMDDTHHMMSSZ.json     # raw Open-Meteo snapshots

processed-weather/
└── history/
    └── historical.csv                # consolidated training dataset

models/
└── temperature/
    ├── model.pkl                     # serialized scikit-learn Pipeline
    ├── threshold.json                # {"rule": "2x_rmse", "threshold": ...}
    └── schema.json                   # feature columns, defaults, categoricals, target
```

---

## REST API

Base URL: `http://localhost:5001`

### `GET /check_health`
Liveness check.
```json
{"status": "ok"}
```

### `POST /train`
Runs the full training pipeline and uploads the artifacts.

| Status | Meaning |
|--------|---------|
| `200`  | Trained — returns `rmse`, `threshold_data`, `rule` |
| `500`  | Training failed — returns `error`, `detail` |

### `GET /schema`
Returns what the model expects:

```json
{
  "target": "temperature_2m",
  "feature_columns": ["relative_humidity_2m", "dew_point_2m", "...", "quarter"],
  "categorical_features": ["weather_code", "is_day"],
  "defaults": {"relative_humidity_2m": 67.45, "hour": 9.0, "...": "..."}
}
```

### `POST /predict`
**Body**
```json
{"features": {"hour": 16, "weather_code": 2}}
```

| Status | Meaning |
|--------|---------|
| `200`  | Returns `predicted_temperature_2m`, `horizon_hours`, `threshold`, `rule` |
| `400`  | Payload is not JSON or has no `features` object |
| `409`  | No trained model found — run `POST /train` first |

---

## Messaging & Alerting (RabbitMQ)

### Queues

| Queue                    | Durable | Purpose |
|--------------------------|---------|---------|
| `weather.observations`   | yes     | Incoming observations (features + actual temperature) |
| `alerts.temp_deviation`  | yes     | Outgoing alerts when the prediction error is too large |

### Observation message

```json
{
  "timestamp": "2026-02-11T20:00:00Z",
  "features": {"hour": 12, "weather_code": 2},
  "actual_temperature_2m": 27
}
```

`actual_temperature_2m` is **required** for alerting — without it the error cannot be computed.

### Alert rule

```
error = |actual_temperature_2m − predicted_temperature_2m|
alert if error > threshold      (threshold = 2 × RMSE)
```

### Alert message

```json
{
  "status": "ALARM",
  "predicted_temperature_2m": 23.19,
  "actual_temperature_2m": 27.0,
  "error": 3.81,
  "threshold": 0.293,
  "rule": "2x_rmse",
  "timestamp": "2026-02-11T20:00:00Z"
}
```

### Consumer reliability

- **Connection retries** — the consumer retries the RabbitMQ connection (default 30 attempts, 2 s apart) so it can start before the broker is ready.
- **`prefetch_count=1`** — processes one message at a time.
- **Manual acknowledgements**
  - malformed JSON / missing `features` → `ack` (dropped, not retried)
  - API unreachable or `5xx` → `nack` with `requeue=True` (retried)
  - API `4xx` → `ack` (client error, not retried)
- Alerts are published with `delivery_mode=2` (persistent).

---

## Machine Learning Pipeline

### Target & features

- **Target:** `temperature_2m`
- **Features:** all other weather variables (humidity, dew point, apparent temperature, precipitation, rain, snowfall, pressure, cloud cover, wind speed/direction/gusts, soil temperature/moisture, `weather_code`, `is_day`, …) plus time features derived from the date:
  `year`, `month`, `day`, `hour`, `weekday`, `weekofyear`, `quarter`
- The target is never included in the feature set, to avoid data leakage.

### Preprocessing (`ColumnTransformer`)

| Feature type | Steps |
|--------------|-------|
| Numeric      | `SimpleImputer(strategy="median")` → `StandardScaler()` |
| Categorical (`weather_code`, `is_day`) | `SimpleImputer(strategy="most_frequent")` → `OneHotEncoder(handle_unknown="ignore")` |

### Model

```
Pipeline([
    ("preprocessor", ColumnTransformer(...)),
    ("regressor",    LinearRegression()),
])
```

- Train/test split: 80/20, `random_state=42`
- Metric: **RMSE** on the test set
- Alert threshold: **2 × RMSE**

Wrapping preprocessing and the model in one `Pipeline` guarantees that the exact same transformations are applied during training and serving.

### Default values

`schema.json` stores a default value for every feature (computed from the training data). When a prediction request contains only a few features, the missing ones are filled from these defaults so that the model always receives a complete, correctly ordered input row.

---

## Configuration

All settings are provided as environment variables (set in `docker-compose.yml`).

| Variable             | Description                          | Example / Default |
|----------------------|--------------------------------------|-------------------|
| `MINIO_ENDPOINT`     | MinIO endpoint URL                   | `http://minio:9000` |
| `MINIO_ACCESS_KEY`   | MinIO access key                     | — |
| `MINIO_SECRET_KEY`   | MinIO secret key                     | — |
| `MINIO_BUCKET_RAW`   | Bucket for raw snapshots             | `raw-weather` |
| `MODEL_BUCKET`       | Bucket for model artifacts           | `models` |
| `MODEL_KEY`          | Object key of the model              | `temperature/model.pkl` |
| `THRESHOLD_KEY`      | Object key of the threshold          | `temperature/threshold.json` |
| `SCHEMA_KEY`         | Object key of the schema             | `temperature/schema.json` |
| `LATITUDE`           | Location latitude                    | `38.2466` (Patras) |
| `LONGITUDE`          | Location longitude                   | `21.7346` (Patras) |
| `PAST_HOURS`         | Hours of history to fetch per run    | `24` |
| `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD` | MinIO admin credentials | — |
| `RABBITMQ_DEFAULT_USER` / `RABBITMQ_DEFAULT_PASS` | RabbitMQ credentials | — |

> ⚠️ Do not commit real credentials. Use a `.env` file (added to `.gitignore`) for anything beyond local testing.

### Ports

| Service  | Port(s) |
|----------|---------|
| MinIO    | `9000` (API), `9001` (console) |
| RabbitMQ | `5672` (AMQP), `15672` (management UI) |
| Flask    | `5001` |

---

## Project Structure

```
.
├── docker-compose.yml
├── data_ingestion.py     # MinIO client + Open-Meteo ingestion
├── processor.py          # raw JSON → historical.csv
├── client.py             # training pipeline & artifact upload
├── main.py               # Flask API (serving layer)
├── rabbit.py             # RabbitMQ consumer & alert publisher
├── requirements.txt
└── README.md
```

---

## Limitations & Future Work

**Current limitations**
- Linear Regression is a deliberately simple baseline; predictions are illustrative rather than accurate forecasts.
- The train/test split is random, not time-based, so evaluation does not fully reflect real forecasting conditions.
- Filling missing features with defaults keeps the system running but can bias predictions.
- Ingestion and processing are triggered manually (no scheduler).
- Artifacts are reloaded from MinIO on every prediction (no caching).

**Possible improvements**
- Use a time-series split and lag/rolling features; try gradient boosting or sequence models.
- Schedule ingestion, processing and retraining (cron, Airflow, Prefect).
- Cache the model in memory and reload only when a new version is uploaded.
- Add model versioning (e.g. MLflow) and monitoring dashboards.
- Add automated tests and a CI/CD pipeline.
- Deploy to a managed cloud environment (AWS S3 / Amazon MQ / ECS or Kubernetes).
