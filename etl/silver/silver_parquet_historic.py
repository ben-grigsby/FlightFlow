# etl/silver/silver_parquet_historic.py

import glob
import os
import json
import pandas as pd
import pyarrow as pa

from pathlib import Path
from tqdm import tqdm
from pyarrow import parquet as pq
from pyarrow import Table

INPUT_DIR = 'data/kafka_logs/avstack'
OUTPUT_DIR = 'data/iceberg_parquet'

os.makedirs(OUTPUT_DIR, exist_ok=True)

def convert_json_to_df(filepath):
    """
    Flattening the JSON files and creating a pandas DataFrame from them which we return.
    """
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
            "flight_date": flight.get("flight_date") or None,
            "flight_status": flight.get("flight_status") or None,

            # Departure
            "dept_airport": dept.get("airport") or None,
            "dept_timezone": dept.get("timezone") or None,
            "dept_airport_iata": dept.get("iata") or None,
            "dept_airport_icao": dept.get("icao") or None,
            "dept_airport_term": dept.get("terminal") or None,
            "dept_airport_gate": dept.get("gate") or None,
            "dept_airport_delay": dept.get("delay") or None,
            "dept_scheduled": dept.get("scheduled") or None,
            "dept_estimated": dept.get("estimated") or None,
            "dept_actual": dept.get("actual") or None,
            "dept_est_runway": dept.get("estimated_runway") or None,
            "dept_act_runway": dept.get("actual_runway") or None,

            # Arrival
            "arr_airport": arr.get("airport") or None,
            "arr_timezone": arr.get("timezone") or None,
            "arr_airport_iata": arr.get("iata") or None,
            "arr_airport_icao": arr.get("icao") or None,
            "arr_airport_term": arr.get("terminal") or None,
            "arr_airport_gate": arr.get("gate") or None,
            "arr_baggage": arr.get("baggage") or None,
            "arr_scheduled": arr.get("scheduled") or None,
            "arr_delay": arr.get("delay") or None,
            "arr_estimated": arr.get("estimated") or None,
            "arr_actual": arr.get("actual") or None,
            "arr_est_runway": arr.get("estimated_runway") or None,
            "arr_act_runway": arr.get("actual_runway") or None,

            # Airline
            "airline_name": airline.get("name") or None,
            "airline_iata": airline.get("iata") or None,
            "airline_icao": airline.get("icao") or None,

            # Flight info
            "flight_number": flight_info.get("number") or None,
            "flight_iata": flight_info.get("iata") or None,
            "flight_icao": flight_info.get("icao") or None,

            # Aircraft
            "aircraft_reg": aircraft.get("registration") or None,
            "aircraft_iata": aircraft.get("iata") or None,
            "aircraft_icao": aircraft.get("icao")
        }
    
        rows.append(row)

    df = pd.DataFrame(rows)
    return df



def write_to_parquet(df):
    """
    Append or create parquet file for the given date
    """
    date = pd.to_datetime(df['flight_date'].iloc[0])
    month, day = f"{date.month:02d}", f"{date.day:02d}"
    out_dir = Path(OUTPUT_DIR) / f"month={month}" / f"day={day}"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "flights.parquet"

    if out_path.exists():
        existing = pd.read_parquet(out_path)
        df = pd.concat([existing, df], ignore_index=True) #.drop_duplicates()
    df = df.astype(str)
    df.to_parquet(out_path, index=False, compression="snappy")
    print(f"Saved {len(df)} rows → {out_path}")



def sanitize_nested(df):
    """Convert any nested dicts/lists into JSON strings to avoid parquet/hash errors."""
    for col in df.columns:
        df[col] = df[col].apply(
            lambda x: json.dumps(x) if isinstance(x, (dict, list)) else x
        )
    return df



def process_json_files():
    buffer_df = pd.DataFrame()
    
    for file in tqdm(glob.glob(os.path.join(INPUT_DIR, "*.json"))):
        print(file)
        df = convert_json_to_df(file)
        if df.empty:
            continue

        #Sanitize the new chunk before merging
        df = sanitize_nested(df)
            
        buffer_df = pd.concat([buffer_df, df], ignore_index=True)
        unique_dates = buffer_df["flight_date"].unique()

        if len(unique_dates) > 2:
            earliest_date = min(unique_dates)
            df_to_write = buffer_df[buffer_df['flight_date'] == earliest_date]

            #Sanitize again before saving (extra safety)
            df_to_write = sanitize_nested(df_to_write)
            write_to_parquet(df_to_write)

            buffer_df = buffer_df[buffer_df['flight_date'] != earliest_date]
        
    if not buffer_df.empty:
        for d in buffer_df['flight_date'].unique():
            df_to_write = buffer_df[buffer_df['flight_date'] == d]
            df_to_write = sanitize_nested(df_to_write)
            write_to_parquet(df_to_write)
        

if __name__ == '__main__':
    print("Starting JSON -> parquet conversion")
    process_json_files()
    print("Completed JSON -> parquet conversion")