import os
import json
from datetime import datetime, timezone
import requests
import boto3
from botocore.client import Config


OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"


def fetch_current_and_past_hours(latitude: float, longitude: float, past_hours: int = 24) -> dict:
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "timezone": "Europe/Athens",
        "current": ",".join([
            "temperature_2m",
            "relative_humidity_2m",
            "dew_point_2m",
            "apparent_temperature",
            "precipitation",
            "rain",
            "showers",
            "snowfall",
            "weather_code",
            "pressure_msl",
            "surface_pressure",
            "cloud_cover",
            "wind_speed_10m",
            "wind_direction_10m",
            "wind_gusts_10m",
            "is_day",
        ]),
        "hourly": ",".join([
            "temperature_2m",
            "relative_humidity_2m",
            "dew_point_2m",
            "apparent_temperature",
            "precipitation",
            "rain",
            "showers",
            "snowfall",
            "weather_code",
            "pressure_msl",
            "surface_pressure",
            "cloud_cover",
            "wind_speed_10m",
            "wind_direction_10m",
            "wind_gusts_10m",
            "is_day",
        ]),
        "past_hours": past_hours,
        # Keep the response tight: no multi-day forecasts
        "forecast_hours": 0,
    }

    resp = requests.get(OPEN_METEO_URL, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()


def get_s3_client():
    endpoint = os.environ["MINIO_ENDPOINT"]          
    access_key = os.environ["MINIO_ACCESS_KEY"]      
    secret_key = os.environ["MINIO_SECRET_KEY"]      
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


def upload_json(bucket: str, key: str, payload: dict):
    s3 = get_s3_client()
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=body,
        ContentType="application/json",
    )


def main():
    bucket = os.environ.get("MINIO_BUCKET_RAW", "raw-weather")

    lat = float(os.environ.get("LATITUDE", "38.2466"))    # Patras default
    lon = float(os.environ.get("LONGITUDE", "21.7346"))

    data = fetch_current_and_past_hours(lat, lon, past_hours=int(os.environ.get("PAST_HOURS", "24")))

    now = datetime.now(timezone.utc)
    key = now.strftime("%Y/%m/%d") + "/" + now.strftime("obs_%Y%m%dT%H%M%SZ.json")

    upload_json(bucket=bucket, key=key, payload=data)
    print(f"Uploaded snapshot to s3://{bucket}/{key}")


if __name__ == "__main__":
    main()
