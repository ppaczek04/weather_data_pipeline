import requests
import pandas as pd
import os
from datetime import datetime, timezone

# Lista miast z koordynatami
CITIES = [
    {"city": "Warsaw", "lat": 52.23, "lon": 21.01},
    {"city": "Krakow", "lat": 50.06, "lon": 19.94},
]

API_URL = "https://api.open-meteo.com/v1/forecast"

def fetch_weather(city):
    """Pobiera dane pogodowe z Open-Meteo dla danego miasta"""
    params = {
        "latitude": city["lat"],
        "longitude": city["lon"],
        "hourly": "temperature_2m,relative_humidity_2m,precipitation",
        "timezone": "UTC",
        "past_days": 0
    }

    response = requests.get(API_URL, params=params, timeout=30)
    response.raise_for_status()
    data = response.json()

    df = pd.DataFrame({
        "timestamp": data["hourly"]["time"],
        "temperature_c": data["hourly"]["temperature_2m"],
        "humidity_pct": data["hourly"]["relative_humidity_2m"],
        "precip_mm": data["hourly"]["precipitation"],
    })
    df["city"] = city["city"]
    df["latitude"] = city["lat"]
    df["longitude"] = city["lon"]
    df["load_dts_utc"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    print(f"Fetched {len(df)} rows for {city['city']}")
    return df

def main():
    all_data = pd.concat([fetch_weather(c) for c in CITIES], ignore_index=True)

    
    os.makedirs("data", exist_ok=True)

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    filename_with_timestamp_id = f"weather_data_{timestamp}.csv"
    full_saving_data_path = os.path.join("data", filename_with_timestamp_id)
    all_data.to_csv(full_saving_data_path, index=False, encoding="utf-8")
    print("\n✅ Weather data saved to weather_data.csv")

if __name__ == "__main__":
    main()