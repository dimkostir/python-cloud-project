import os
import json
import pandas as pd
from Data_ingestion.data_ingestion import get_s3_client


def list_raw_keys(s3, bucket: str, prefix: str) -> list[str]:
    keys = []
    token = None

    while True:
        kwargs = {"Bucket": bucket, "Prefix": prefix}
        if token:
            kwargs["ContinuationToken"] = token

        resp = s3.list_objects_v2(**kwargs)
        for obj in resp.get("Contents", []):
            keys.append(obj["Key"])

        if resp.get("IsTruncated"):
            token = resp.get("NextContinuationToken")
        else:
            break

    return keys


def raw_json_to_rows(raw: dict) -> list[dict]:
    rows = []
    hourly = raw.get("hourly", {})
    times = hourly.get("time", [])
    if not times:
        return rows

    # Take all hourly variables except "time"
    hourly_vars = [k for k in hourly.keys() if k != "time"]

    for i, t in enumerate(times):
        dt = pd.to_datetime(t)
        # Keep every 3 hours
        if dt.hour % 3 != 0:
            continue

        row = {"date": dt.isoformat()}
        for var in hourly_vars:
            vals = hourly.get(var, [])
            if i < len(vals):
                row[var] = vals[i]
        rows.append(row)

    return rows


def build_new_df_from_raw_bucket(s3, raw_bucket: str, raw_prefix: str) -> pd.DataFrame:
    keys = list_raw_keys(s3, raw_bucket, raw_prefix)

    all_rows = []
    for key in keys:
        if not key.endswith(".json"):
            continue

        local_path = "/tmp/raw.json"
        s3.download_file(raw_bucket, key, local_path)

        with open(local_path, "r") as f:
            raw = json.load(f)

        all_rows.extend(raw_json_to_rows(raw))

    if not all_rows:
        return pd.DataFrame()

    df_new = pd.DataFrame(all_rows)
    df_new["date"] = pd.to_datetime(df_new["date"])
    df_new = df_new.sort_values("date").drop_duplicates(subset=["date"], keep="last")
    return df_new


def upsert_historical_csv(s3, processed_bucket: str, historical_key: str, df_new: pd.DataFrame) -> int:
    if df_new.empty:
        return 0

    local_hist = "/tmp/historical.csv"

    # Try download existing historical.csv
    try:
        s3.download_file(processed_bucket, historical_key, local_hist)
        df_old = pd.read_csv(local_hist)
        df_old["date"] = pd.to_datetime(df_old["date"])
        df_all = pd.concat([df_old, df_new], ignore_index=True)
    except Exception:
        # If not exists, create new
        df_all = df_new.copy()
        
    df_all["date"] = pd.to_datetime(df_all["date"], utc=True).dt.tz_convert(None)

    df_all = df_all.sort_values("date").drop_duplicates(subset=["date"], keep="last")
    df_all.to_csv(local_hist, index=False)

    s3.upload_file(local_hist, processed_bucket, historical_key)
    return len(df_new)


def main():
    s3 = get_s3_client()

    raw_bucket = os.environ["RAW_BUCKET"]                 # raw-weather
    raw_prefix = os.environ.get("RAW_PREFIX", "")         # optional
    processed_bucket = os.environ["PROCESSED_BUCKET"]     # processed-weather
    historical_key = os.environ["HISTORICAL_KEY"]         # history/historical.csv

    df_new = build_new_df_from_raw_bucket(s3, raw_bucket, raw_prefix)
    added = upsert_historical_csv(s3, processed_bucket, historical_key, df_new)

    print(f"Processor finished. New rows considered: {added}. Historical updated at {processed_bucket}/{historical_key}")


if __name__ == "__main__":
    main()
