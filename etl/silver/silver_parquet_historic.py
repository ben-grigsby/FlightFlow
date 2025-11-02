# etl/silver/silver_parquet_historic.py

import glob
import os
import json
import pandas as pd
from pathlib import Path
from tqdm import tqdm
from pyarrow import parquet as pq


# ===========================================================
# CONFIGURATION (now fully Airflow-based)
# ===========================================================
BASE_DIR = "/opt/airflow"
INPUT_DIR = os.path.join(BASE_DIR, "data/kafka_logs/avstack")
OUTPUT_DIR = os.path.join(BASE_DIR, "data/iceberg_parquet")

os.makedirs(OUTPUT_DIR, exist_ok=True)


# ===========================================================
# JSON → DataFrame
# ===========================================================
def convert_json_to_df(filepath):
    """Flatten JSON flight data into a DataFrame."""
    with open(filepath) as f:
        content = json.load(f)

    if not content:
        return pd.DataFrame()

    rows = []
    for flight in content:
        dept = flight.get("departure", {}) or {}
        arr = flight.get("arrival", {}) or {}
        airline = flight.get("airline", {}) or {}
        flight_info = flight.get("flight", {}) or {}
        aircraft = flight.get("aircraft", {}) or {}

        row = {
            "flight_date": flight.get("flight_date"),
            "flight_status": flight.get("flight_status"),
            # Departure
            "dept_airport": dept.get("airport"),
            "dept_timezone": dept.get("timezone"),
            "dept_airport_iata": dept.get("iata"),
            "dept_airport_icao": dept.get("icao"),
            "dept_airport_term": dept.get("terminal"),
            "dept_airport_gate": dept.get("gate"),
            "dept_airport_delay": dept.get("delay"),
            "dept_scheduled": dept.get("scheduled"),
            "dept_estimated": dept.get("estimated"),
            "dept_actual": dept.get("actual"),
            "dept_est_runway": dept.get("estimated_runway"),
            "dept_act_runway": dept.get("actual_runway"),
            # Arrival
            "arr_airport": arr.get("airport"),
            "arr_timezone": arr.get("timezone"),
            "arr_airport_iata": arr.get("iata"),
            "arr_airport_icao": arr.get("icao"),
            "arr_airport_term": arr.get("terminal"),
            "arr_airport_gate": arr.get("gate"),
            "arr_baggage": arr.get("baggage"),
            "arr_scheduled": arr.get("scheduled"),
            "arr_delay": arr.get("delay"),
            "arr_estimated": arr.get("estimated"),
            "arr_actual": arr.get("actual"),
            "arr_est_runway": arr.get("estimated_runway"),
            "arr_act_runway": arr.get("actual_runway"),
            # Airline
            "airline_name": airline.get("name"),
            "airline_iata": airline.get("iata"),
            "airline_icao": airline.get("icao"),
            # Flight info
            "flight_number": flight_info.get("number"),
            "flight_iata": flight_info.get("iata"),
            "flight_icao": flight_info.get("icao"),
            # Aircraft
            "aircraft_reg": aircraft.get("registration"),
            "aircraft_iata": aircraft.get("iata"),
            "aircraft_icao": aircraft.get("icao"),
        }

        rows.append(row)

    return pd.DataFrame(rows)


# ===========================================================
# SANITIZATION + WRITING
# ===========================================================
def sanitize_nested(df):
    """Convert nested dicts/lists into JSON strings."""
    df = df.copy()
    for col in df.columns:
        df.loc[:, col] = df[col].apply(
            lambda x: json.dumps(x) if isinstance(x, (dict, list)) else x
        )
    return df


def write_to_parquet(df):
    """Append or create Parquet partition for given date."""
    date = pd.to_datetime(df["flight_date"].iloc[0])
    month, day = f"{date.month:02d}", f"{date.day:02d}"
    out_dir = Path(OUTPUT_DIR) / f"month={month}" / f"day={day}"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "flights.parquet"

    if out_path.exists():
        existing = pd.read_parquet(out_path)
        df = pd.concat([existing, df], ignore_index=True)

    df = df.astype(str)
    df.to_parquet(out_path, index=False, compression="snappy")
    print(f"✅ Saved {len(df)} rows → {out_path}")


# ===========================================================
# MAIN PROCESS
# ===========================================================
def process_json_files():
    buffer_df = pd.DataFrame()

    for file in tqdm(glob.glob(os.path.join(INPUT_DIR, "*.json"))):
        print(f"Processing: {file}")
        try:
            df = convert_json_to_df(file)
            if df.empty:
                print(f"Skipped empty file: {file}")
                continue

            df = sanitize_nested(df)
            buffer_df = pd.concat([buffer_df, df], ignore_index=True)
            unique_dates = buffer_df["flight_date"].unique()

            # Write and flush oldest date partition to Parquet
            if len(unique_dates) > 2:
                earliest_date = min(unique_dates)
                df_to_write = buffer_df.loc[buffer_df["flight_date"] == earliest_date].copy()
                df_to_write = sanitize_nested(df_to_write)
                write_to_parquet(df_to_write)
                buffer_df = buffer_df.loc[buffer_df["flight_date"] != earliest_date].copy()

            # Delete the file only after successful processing
            os.remove(file)
            print(f"🗑️ Deleted processed JSON: {file}")

        except Exception as e:
            print(f"Error processing {file}: {e}")

    # Process any remaining buffered data
    if not buffer_df.empty:
        for d in buffer_df["flight_date"].unique():
            df_to_write = buffer_df.loc[buffer_df["flight_date"] == d].copy()
            df_to_write = sanitize_nested(df_to_write)
            write_to_parquet(df_to_write)

    print("All JSON files processed and cleaned up.")


if __name__ == "__main__":
    print("🚀 Starting JSON → Parquet conversion")
    process_json_files()
    print("🎯 Completed JSON → Parquet conversion")