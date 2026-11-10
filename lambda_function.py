from weather_data_extract import extract_weather
from transform_weather_data import transform_weather

def lambda_handler(event, context):
    print("Starting NYC Weather Pipeline")

    try:
        #Step 1: fetch weather data and upload raw JSON to s3
        print("Step 1: Extracting weather data...")
        extract_weather()

        # Step 2: Transform raw JSON into Parquet
        print("Step 2: Transforming weather data...")
        transform_weather()

        print("NYC Weather Pipeline completed successfully!")

    # Step 2: Call transformation function
        return {
        "statusCode": 200,
        "message": "Weather pipeline completed"
         }
    except Exception as error:
        print(f"Weather pipeline failed: {error}")
        raise