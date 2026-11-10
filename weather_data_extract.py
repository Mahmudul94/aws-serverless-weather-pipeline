
import json
from datetime import datetime
from zoneinfo import ZoneInfo

import boto3
import requests
from botocore.exceptions import ClientError

BUCKET_NAME = "nyc-weather-pipeline-unique124"
URL = "https://api.open-meteo.com/v1/forecast"

boroughs = {
    "Queens": (40.7282, -73.7949),
    "Bronx": (40.8448, -73.8648),
    "Brooklyn": (40.6782, -73.9442),
    "Manhattan": (40.7831, -73.9712),
    "Staten Island": (40.5795, -74.1502)
}


def extract_weather():
    s3 = boto3.client("s3", region_name="us-east-2")

    today = datetime.now(
        ZoneInfo("America/New_York")
    ).date().isoformat()

    uploaded_count = 0
    skipped_count = 0

    for borough, coordinates in boroughs.items():

        latitude, longitude = coordinates

        params = {
            "latitude": latitude,
            "longitude": longitude,
            "current": "temperature_2m,relative_humidity_2m,wind_speed_10m",
            "timezone": "America/New_York"
        }

        response = requests.get(
            URL,
            params=params,
            timeout=15
        )
        response.raise_for_status()

        weather_data = response.json()

        filename = borough.lower().replace(" ", "_") + ".json"

        object_key = (
            f"raw/observation_date={today}/{filename}"
        )

        try:
            s3.put_object(
                Bucket=BUCKET_NAME,
                Key=object_key,
                Body=json.dumps(weather_data),
                ContentType="application/json",
                IfNoneMatch="*"
            )

            print(f"Uploaded: {object_key}")
            uploaded_count += 1

        except ClientError as error:
            if error.response["Error"]["Code"] in (
                "PreconditionFailed",
                "412"
            ):
                print(f"Skipped existing: {object_key}")
                skipped_count += 1
            else:
                raise

    return {
        "uploaded_count": uploaded_count,
        "skipped_count": skipped_count
    }


if __name__ == "__main__":
    print(extract_weather())
 