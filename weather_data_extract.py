import json
from pathlib import Path
import pandas as pd
import boto3
from botocore.exceptions import ClientError
from datetime import datetime
from zoneinfo import ZoneInfo
import requests

url = "https://api.open-meteo.com/v1/forecast"
output_dir = Path(__file__).resolve().parent


session =boto3.Session(
    profile_name = "weather-dev",
    region_name ="us-east-2"
)
s3 =session.client("s3")

#Adding todays Date
today = datetime.now(
    ZoneInfo("America/New_York")
).date().isoformat()

boroughs = {
    "Queens": (40.7282, -73.7949),
    "Bronx": (40.8448, -73.8648),
    "Brooklyn": (40.6782, -73.9442),
    "Manhattan": (40.7831, -73.9712),
    "Staten Island": (40.5795, -74.1502)
}
for boroughs, coordinates in boroughs.items():
        filename = boroughs.lower().replace(" ", "_") + ".json"
        object_key = f"raw/observation_date={today}/{filename}"
        try:
            s3.head_object(
            Bucket="nyc-weather-pipeline-unique124",
            Key=object_key
        )
            print(f"SKIPPED: {boroughs} already uploaded today")
            continue
        except ClientError as e:
            if e.response["Error"]["Code"] not in (
            "404", "NoSuchKey", "NotFound"
        ):
             raise
        
        latitude, longitude = coordinates
        params = {
            "latitude": latitude,
            "longitude": longitude,
            "current": "temperature_2m,relative_humidity_2m,wind_speed_10m",
            "timezone": "America/New_York"
        }
        response = requests.get(url, params=params, timeout=15)
        response.raise_for_status()
        weather_data = response.json()
        observation_date = weather_data["current"]["time"][:10]
        object_key = f"raw/observation_date={observation_date}/{boroughs}.json"
        json_body = json.dumps(weather_data)
        s3.put_object(
            Bucket="nyc-weather-pipeline-unique124",
            Key=object_key,
            Body=json_body,
            ContentType="application/json"
        )
        print("Sucessfully Uploaded")
        