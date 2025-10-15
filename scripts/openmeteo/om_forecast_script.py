# scripts/openmeteo/om_forecast_script.py

from dotenv import load_dotenv
from kafka import KafkaProducer

import sys
import os
import json
import requests
import datetime
import pandas as pd

load_dotenv(dotenv_path="/opt/airflow/.env")

# ==================================================================
# Action
# ==================================================================



def get_jfk_weather_forecast():
    url = "https://api.open-meteo.com/v1/forecast"

    params = {
        "latitude": 40.6413,   # JFK Airport
        "longitude": -73.7781,
        "daily": [
            "temperature_2m_max",
            "temperature_2m_min",
            "precipitation_sum",
            "rain_sum",
            "snowfall_sum",
            "windspeed_10m_max",
            "windgusts_10m_max"
        ],
        "timezone": "America/New_York",
        "forecast_days": 14
    }

    print("📡 Requesting forecast from Open-Meteo…")
    response = requests.get(url, params=params)
    print("Status:", response.status_code)

    data = response.json()
    # Put into DataFrame for easy handling
    daily = pd.DataFrame(data["daily"])
    print(daily.head())

    return daily

if __name__ == "__main__":
    df = get_jfk_weather_forecast()
    df.to_csv("jfk_weather_forecast.csv", index=False)
    print("Saved 14-day forecast to jfk_weather_forecast.csv")