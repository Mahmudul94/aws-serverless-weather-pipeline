
import boto3
import json
from datetime import datetime
import pandas as pd
from zoneinfo import ZoneInfo
from botocore.exceptions import ClientError


BUCKET_NAME = "nyc-weather-pipeline-unique124"
REGION = "us-east-2"

BOROUGHS = [
    "Queens",
    "Bronx",
    "Brooklyn",
    "Manhattan",
    "Staten Island"
]


def transform_weather():
    # Lambda automatically uses its IAM execution role
    s3 = boto3.client("s3", region_name=REGION)

    observation_date = datetime.now(
        ZoneInfo("America/New_York")
    ).strftime("%Y-%m-%d")

    json_data_list = []

    # Step 1: Read raw JSON files from S3
    for borough in BOROUGHS:

        filename = borough.lower().replace(" ", "_") + ".json"

        object_key = (
            f"raw/observation_date={observation_date}/{filename}"
        )

        response = s3.get_object(
            Bucket=BUCKET_NAME,
            Key=object_key
        )

        weather_data = json.loads(response["Body"].read())

        record = {
            "borough": borough,
            "observation_date": weather_data["current"]["time"][:10],
            "observation_time": weather_data["current"]["time"],
            "temperature_2m": weather_data["current"]["temperature_2m"],
            "relative_humidity_2m": weather_data["current"]["relative_humidity_2m"],
            "wind_speed_10m": weather_data["current"]["wind_speed_10m"]
        }

        json_data_list.append(record)

    # Step 2: Convert JSON records to DataFrame
    data = pd.DataFrame(json_data_list)

    data["observation_date"] = pd.to_datetime(
        data["observation_date"]
    ).dt.date

    data["observation_time"] = pd.to_datetime(
        data["observation_time"]
    ).dt.tz_localize("America/New_York")

    # Step 3: Convert DataFrame to Parquet
    parquet_data = data.to_parquet(
        engine="pyarrow",
        index=False,
        compression="snappy"
    )

    # Step 4: Upload Parquet to S3
    curated_destination = (
        f"curated/observation_date={observation_date}/weather.parquet"
    )

    try:
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=curated_destination,
            Body=parquet_data,
            ContentType="application/vnd.apache.parquet",
            IfNoneMatch="*"
        )

        print(f"Successfully uploaded: {curated_destination}")

        return {
            "status": "uploaded",
            "records": len(data),
            "s3_key": curated_destination
        }

    except ClientError as error:
        if error.response["Error"]["Code"] in (
            "PreconditionFailed",
            "412"
        ):
            print(f"Skipped existing file: {curated_destination}")

            return {
                "status": "skipped",
                "records": len(data),
                "s3_key": curated_destination
            }

        raise


if __name__ == "__main__":
    print(transform_weather())
