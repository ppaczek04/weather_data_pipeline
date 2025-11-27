import requests
import pandas as pd
import os
from datetime import datetime, timezone, timedelta

# List of cities with coordinates
CITIES = [
    {"city": "Montgomery", "state": "Alabama", "lat": 32.377716, "lon": -86.300568},
    {"city": "Juneau", "state": "Alaska", "lat": 58.301598, "lon": -134.420212},
    {"city": "Phoenix", "state": "Arizona", "lat": 33.448143, "lon": -112.096962},
    {"city": "Little Rock", "state": "Arkansas", "lat": 34.746613, "lon": -92.288986},
    {"city": "Sacramento", "state": "California", "lat": 38.576668, "lon": -121.493629},
    {"city": "Denver", "state": "Colorado", "lat": 39.739227, "lon": -104.984856},
    {"city": "Hartford", "state": "Connecticut", "lat": 41.764046, "lon": -72.682198},
    {"city": "Dover", "state": "Delaware", "lat": 39.157307, "lon": -75.519722},
    {"city": "Tallahassee", "state": "Florida", "lat": 30.438118, "lon": -84.281296},
    {"city": "Atlanta", "state": "Georgia", "lat": 33.749027, "lon": -84.388229},
    {"city": "Honolulu", "state": "Hawaii", "lat": 21.307442, "lon": -157.857376},
    {"city": "Boise", "state": "Idaho", "lat": 43.617775, "lon": -116.199722},
    {"city": "Springfield", "state": "Illinois", "lat": 39.798363, "lon": -89.654961},
    {"city": "Indianapolis", "state": "Indiana", "lat": 39.768623, "lon": -86.162643},
    {"city": "Des Moines", "state": "Iowa", "lat": 41.591087, "lon": -93.603729},
    {"city": "Topeka", "state": "Kansas", "lat": 39.048191, "lon": -95.677956},
    {"city": "Frankfort", "state": "Kentucky", "lat": 38.186722, "lon": -84.875374},
    {"city": "Baton Rouge", "state": "Louisiana", "lat": 30.457069, "lon": -91.187393},
    {"city": "Augusta", "state": "Maine", "lat": 44.307167, "lon": -69.781693},
    {"city": "Annapolis", "state": "Maryland", "lat": 38.978764, "lon": -76.490936},
    {"city": "Boston", "state": "Massachusetts", "lat": 42.358162, "lon": -71.063698},
    {"city": "Lansing", "state": "Michigan", "lat": 42.733635, "lon": -84.555328},
    {"city": "St. Paul", "state": "Minnesota", "lat": 44.955097, "lon": -93.102211},
    {"city": "Jackson", "state": "Mississippi", "lat": 32.303848, "lon": -90.182106},
    {"city": "Jefferson City", "state": "Missouri", "lat": 38.576702, "lon": -92.173516},
    {"city": "Helena", "state": "Montana", "lat": 46.585709, "lon": -112.018417},
    {"city": "Lincoln", "state": "Nebraska", "lat": 40.808075, "lon": -96.699654},
    {"city": "Carson City", "state": "Nevada", "lat": 39.163914, "lon": -119.766121},
    {"city": "Concord", "state": "New Hampshire", "lat": 43.208137, "lon": -71.537567},
    {"city": "Trenton", "state": "New Jersey", "lat": 40.220596, "lon": -74.769913},
    {"city": "Santa Fe", "state": "New Mexico", "lat": 35.686973, "lon": -105.937798},
    {"city": "Albany", "state": "New York", "lat": 42.652843, "lon": -73.757874},
    {"city": "Raleigh", "state": "North Carolina", "lat": 35.78043, "lon": -78.639099},
    {"city": "Bismarck", "state": "North Dakota", "lat": 46.82085, "lon": -100.783318},
    {"city": "Columbus", "state": "Ohio", "lat": 39.961346, "lon": -82.999069},
    {"city": "Oklahoma City", "state": "Oklahoma", "lat": 35.492207, "lon": -97.503342},
    {"city": "Salem", "state": "Oregon", "lat": 44.938461, "lon": -123.030403},
    {"city": "Harrisburg", "state": "Pennsylvania", "lat": 40.264378, "lon": -76.883598},
    {"city": "Providence", "state": "Rhode Island", "lat": 41.830914, "lon": -71.414963},
    {"city": "Columbia", "state": "South Carolina", "lat": 34.000343, "lon": -81.033211},
    {"city": "Pierre", "state": "South Dakota", "lat": 44.367031, "lon": -100.346405},
    {"city": "Nashville", "state": "Tennessee", "lat": 36.16581, "lon": -86.784241},
    {"city": "Austin", "state": "Texas", "lat": 30.27467, "lon": -97.740349},
    {"city": "Salt Lake City", "state": "Utah", "lat": 40.777477, "lon": -111.888237},
    {"city": "Montpelier", "state": "Vermont", "lat": 44.262436, "lon": -72.580536},
    {"city": "Richmond", "state": "Virginia", "lat": 37.538857, "lon": -77.43364},
    {"city": "Olympia", "state": "Washington", "lat": 47.035805, "lon": -122.905014},
    {"city": "Charleston", "state": "West Virginia", "lat": 38.336246, "lon": -81.612328},
    {"city": "Madison", "state": "Wisconsin", "lat": 43.074684, "lon": -89.384445},
    {"city": "Cheyenne", "state": "Wyoming", "lat": 41.140259, "lon": -104.820236},
]


API_URL = "https://api.open-meteo.com/v1/forecast"

def fetch_weather(city):
    """Downloading wather data from Open-Meteo API for each city"""
    yesterday = (datetime.utcnow().date() - timedelta(days=1))
    params = {
        "latitude": city["lat"],
        "longitude": city["lon"],
        "hourly": "temperature_2m,relative_humidity_2m,precipitation",
        "timezone": "UTC",
        "start_date": str(yesterday),
        "end_date": str(yesterday),
        # "past_days": 0
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
    df['state'] = city['state']
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
    print("\n[OK] Weather data saved to weather_data.csv")

if __name__ == "__main__":
    main()