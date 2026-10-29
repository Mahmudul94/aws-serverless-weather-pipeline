import boto3
import json
from datetime import datetime
import pandas as pd
from zoneinfo import ZoneInfo
session = boto3.Session(
    profile_name ="weather-dev",
    region_name ="us-east-2"
)
s3 = session.client("s3")
bucket_name = "nyc-weather-pipeline-unique124"
observation_date=datetime.now(ZoneInfo("America/New_York")).strftime("%Y-%m-%d")
boroughs = ["Queens", "Bronx", "Brooklyn", "Manhattan", "Staten Island"]
json_data_list =[]

for borough in boroughs:
    object_key = f"raw/observation_date={observation_date}/{borough}.json"
    response = s3.get_object(Bucket=bucket_name, Key=object_key)
    weather_data = json.loads(response["Body"].read())
    observation_date = weather_data["current"]["time"][:10]
    observation_time = weather_data["current"]["time"]
    transformded_data = {
        "borough":borough,
        "observation_date":observation_date,
        "observation_time":observation_time,
        "temperature_2m":weather_data["current"]["temperature_2m"],
        "relative_humidity_2m":weather_data["current"]["relative_humidity_2m"],
        "wind_speed_10m":weather_data["current"]["wind_speed_10m"]
    }
    json_data_list.append(transformded_data)
data = pd.DataFrame(json_data_list)
data["observation_date"]= pd.to_datetime(data["observation_date"]).dt.date
data["observation_time"] =pd.to_datetime(data["observation_time"]).dt.tz_localize("America/New_York")
parquet_data = data.to_parquet(
    engine="pyarrow",
    index=False,
    compression="snappy"
)
curated_destination= f"curated/observation_date={observation_date}/weather.parquet"
s3.put_object(
            Bucket="nyc-weather-pipeline-unique124",
            Key=curated_destination,
            Body=parquet_data,
            ContentType="application/vnd.apache.parquet",
            IfNoneMatch="*"
        )
print("Sucessfully upload file in the bucket")