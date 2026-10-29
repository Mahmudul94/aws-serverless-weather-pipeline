# NYC Serverless Weather Data Pipeline

### AWS Data Engineering Project | Python • Amazon S3 • AWS Glue • Amazon Athena • Apache Parquet

## Project Overview

The NYC Serverless Weather Data Pipeline is a cloud-based data engineering project that collects current weather observations for New York City's five boroughs, transforms raw JSON data into structured datasets, and makes the resulting data available for SQL analytics through Amazon Athena.

The project demonstrates fundamental data engineering concepts, including API ingestion, data lake architecture, ETL processing, data transformation, data quality, idempotency, IAM security, lifecycle management, columnar storage, metadata management, and analytical querying.

The pipeline currently runs through Python scripts. Automated deployment and scheduling with AWS Lambda, Amazon EventBridge, and GitHub Actions are planned improvements.

## 1. Project Objectives

- Extract weather observations from a public REST API.
- Collect data for all five New York City boroughs.
- Preserve original JSON responses in Amazon S3.
- Organize data using date-based S3 prefixes.
- Prevent duplicate daily raw and curated uploads.
- Transform nested JSON into a structured Pandas DataFrame.
- Validate data before publishing curated datasets.
- Convert structured data to Apache Parquet.
- Maintain separate raw and curated data layers.
- Implement automatic S3 data retention.
- Register dataset metadata using AWS Glue Data Catalog.
- Query Parquet data directly from Amazon S3 using Athena.
- Prepare the dataset for future business intelligence dashboards.

## 2. Technology Stack

| Technology | Purpose |
|---|---|
| Python 3.12 | Pipeline development |
| Open-Meteo API | Source of weather observations |
| Requests | REST API communication |
| Boto3 | AWS service integration |
| Pandas | Data transformation and validation |
| PyArrow | Parquet serialization |
| Apache Parquet | Columnar analytical storage |
| Amazon S3 | Raw and curated data lake |
| AWS IAM | Identity and access management |
| AWS Glue Data Catalog | Database, table and schema metadata |
| Amazon Athena | Serverless SQL analytics |
| AWS CLI | AWS access and troubleshooting |
| Git and GitHub | Version control |

**AWS Region:** `us-east-2` (Ohio)

## 3. Data Pipeline Architecture

```text
                 Open-Meteo Weather API
                           |
                           v
                 Python API Extraction
                           |
                           v
                  Amazon S3 Raw Layer
                      JSON Files
                           |
                           v
                Python / Pandas ETL
                           |
                  Data Transformation
                           |
                  Data Quality Checks
                           |
                           v
                Apache Parquet Format
                           |
                           v
                Amazon S3 Curated Layer
                           |
                           v
                 AWS Glue Data Catalog
                           |
                           v
                     Amazon Athena
                           |
                           v
                    SQL Analytics
                           |
                           v
               Future BI Dashboard Layer
```

The pipeline follows a layered data lake design:

**Raw Layer:** Stores original weather API responses as JSON.

**Transformation Layer:** Extracts relevant fields, creates structured records, and performs validation.

**Curated Layer:** Stores analytics-ready Parquet data.

**Serving Layer:** Uses AWS Glue Data Catalog and Amazon Athena to make the curated dataset available for SQL queries.

## 4. Weather Data Source

**API Provider:** Open-Meteo

Website: https://open-meteo.com/

The pipeline collects weather observations for:

1. Queens
2. Bronx
3. Brooklyn
4. Manhattan
5. Staten Island

### Weather Attributes

| API Field | Description | Unit |
|---|---|---|
| `time` | Weather observation timestamp | Local datetime |
| `temperature_2m` | Temperature measured at 2 meters | °C |
| `relative_humidity_2m` | Relative humidity | % |
| `wind_speed_10m` | Wind speed measured at 10 meters | km/h |

The pipeline is designed for **daily batch ingestion**, collecting one weather snapshot per borough per day.

The API provides current weather information rather than a complete historical archive.

## 5. Data Extraction

Python retrieves weather information using the Open-Meteo REST API.

The extraction process:

- Sends an HTTP request for each borough.
- Retrieves weather observations in JSON format.
- Preserves source data for subsequent transformation.
- Uses the New York timezone for daily date-based organization.
- Uploads JSON objects into the Amazon S3 raw layer.

The Python `requests` library handles API communication, while Boto3 handles S3 operations.

### Example API Response

```json
{
  "latitude": 40.7283,
  "longitude": -73.80302,
  "timezone": "America/New_York",
  "current": {
    "time": "2026-10-29T12:15",
    "temperature_2m": 18.3,
    "relative_humidity_2m": 60,
    "wind_speed_10m": 12.8
  }
}
```

The raw JSON response preserves the source attributes, units, observation timestamp, and additional API metadata.

## 6. Amazon S3 Data Lake Architecture

**S3 Bucket:** `nyc-weather-pipeline-unique124`

The pipeline uses separate storage prefixes for raw, curated, and Athena query result data.

```text
nyc-weather-pipeline-unique124/
│
├── raw/
│   └── observation_date=2026-10-29/
│       ├── queens.json
│       ├── Bronx.json
│       ├── Brooklyn.json
│       ├── Manhattan.json
│       └── Staten Island.json
│
├── curated/
│   └── observation_date=2026-10-29/
│       └── weather.parquet
│
└── athena-results/
    └── query result files
```

The date-based prefix follows a Hive-style naming convention:

```text
observation_date=YYYY-MM-DD/
```

This design logically organizes daily snapshots and can support date-based partitioning in future Athena table configurations.

### Raw Layer

The raw layer stores one JSON object per borough per observation date.

Benefits include:

- Preserving source responses.
- Supporting debugging and reprocessing during the retention period.
- Separating ingestion from transformation.
- Maintaining traceability to the original weather source.

### Curated Layer

The curated layer combines all five borough records into one structured Parquet file per day.

This avoids creating several extremely small Parquet files for the same daily batch.

Each file contains weather observations for the five boroughs.

## 7. Idempotency and Duplicate Prevention

Idempotency is an important design requirement.

Running the pipeline more than once for the same observation date should not create duplicate datasets or overwrite existing daily snapshots unintentionally.

The pipeline uses date-based S3 object keys to identify existing daily data.

Example:

```text
raw/observation_date=2026-10-29/queens.json
```

For S3 object uploads, the implementation uses conditional write behavior:

```python
IfNoneMatch="*"
```

This instructs Amazon S3 to reject a write when an object already exists at the requested key.

The curated layer follows the same approach:

```text
curated/observation_date=2026-10-29/weather.parquet
```

If the curated file already exists, an attempted conditional upload can return a `412 Precondition Failed` response.

Handling this response as an expected duplicate condition is an important part of reliable pipeline execution.

The dataset also uses the following logical unique key:

```text
observation_date + borough
```

This identifies one borough-level daily snapshot.

## 8. Data Transformation with Pandas

The transformation process reads JSON objects from Amazon S3 using Boto3.

The S3 `GetObject` response contains a streaming body. JSON content is read and converted into Python dictionaries.

The pipeline extracts relevant attributes from the nested `current` structure and creates a structured Pandas DataFrame.

### Curated Dataset Schema

| Column | Description | Data Type |
|---|---|---|
| `borough` | New York City borough | String |
| `observation_date` | Daily observation date | Date |
| `observation_time` | Exact observation timestamp | Timestamp |
| `temperature_2m` | Temperature | Double |
| `relative_humidity_2m` | Relative humidity | Bigint |
| `wind_speed_10m` | Wind speed | Double |

### Example Transformed Dataset

Data collected on October 29, 2026:

| Borough | Temperature (°C) | Humidity (%) | Wind Speed (km/h) |
|---|---:|---:|---:|
| Queens | 18.3 | 60 | 12.8 |
| Bronx | 17.8 | 62 | 12.2 |
| Brooklyn | 17.7 | 65 | 11.7 |
| Manhattan | 17.9 | 60 | 12.2 |
| Staten Island | 16.6 | 72 | 10.9 |

The transformation preserves the original meaningful API field names.

### Date and Timestamp Management

The pipeline distinguishes between:

- `observation_date`: Calendar date used for daily processing and reporting.
- `observation_time`: Complete weather observation timestamp.

New York timezone handling uses `America/New_York`, including daylight saving time adjustments.

For example:

```text
New York local time: 2026-10-29 12:15:00-04:00
UTC equivalent:     2026-10-29 16:15:00
```

Athena displayed the UTC-equivalent observation time during the initial query verification.

Keeping both date and timestamp supports daily reporting, data lineage, and future higher-frequency ingestion.

## 9. Data Quality Validation

Data quality checks are important before publishing datasets to the curated layer.

### Implemented Checks

**Missing Value Validation**

Pandas is used to inspect missing values across the structured dataset.

```python
df.isna().sum()
```

The initial five-borough dataset returned zero missing values across all columns.

### Additional Validation Rules

The following rules are part of the planned production-readiness improvements:

| Rule | Expected Condition |
|---|---|
| Row count | Five borough records per daily batch |
| Completeness | No missing required fields |
| Uniqueness | Unique `borough + observation_date` |
| Borough validation | Only the five expected borough names |
| Humidity validation | Between 0 and 100 |
| Wind speed validation | Non-negative |
| Temperature validation | Numeric data |
| Timestamp consistency | Observation date matches local timestamp date |

The intended design is to stop curated publication when required data quality checks fail.

AWS Glue Data Quality is also being evaluated as an optional future enhancement for rulesets, monitoring, and data quality reporting.

## 10. Apache Parquet Conversion

After transformation, the structured Pandas DataFrame is converted to Apache Parquet.

Parquet is a columnar file format designed for efficient analytical workloads.

### Conversion

```python
parquet_data = df.to_parquet(
    engine="pyarrow",
    compression="snappy",
    index=False
)
```

Parameters:

- `engine="pyarrow"`: Uses PyArrow for serialization.
- `compression="snappy"`: Applies Snappy compression.
- `index=False`: Excludes the Pandas row index.

The initial serialized Parquet dataset was approximately **4.56 KB**.

### Why Parquet?

- Efficient column-oriented storage.
- Data type preservation.
- Compression support.
- Compatibility with Amazon Athena.
- Reduced data scanning for column-specific SQL queries.
- Suitable for analytical workloads.

The Parquet bytes are uploaded directly to S3 using Boto3 without requiring an intermediate local file.

### Upload Configuration

```python
s3.put_object(
    Bucket="nyc-weather-pipeline-unique124",
    Key=curated_destination,
    Body=parquet_data,
    ContentType="application/vnd.apache.parquet",
    IfNoneMatch="*"
)
```

This combines curated storage with conditional-write duplicate protection.

## 11. Read-After-Write Verification

After uploading Parquet data to Amazon S3, the pipeline output was read back successfully using Boto3 and Pandas.

The verification checked:

- Successful S3 object retrieval.
- Parquet readability.
- DataFrame shape.
- Column data types.
- Missing values.
- Preservation of the five borough observations.

This confirms that the uploaded binary file can be read back into a structured dataset.

## 12. S3 Lifecycle Management

Two Amazon S3 Lifecycle Rules are configured.

| Prefix | Retention | Status |
|---|---|---|
| `raw/` | 2 days | Enabled |
| `curated/` | 30 days | Enabled |

### Raw Data Retention

```text
delete-raw-after-2-days
```

Objects under `raw/` become eligible for expiration after two days.

### Curated Data Retention

```text
delete-curated-after-30-days
```

Objects under `curated/` become eligible for expiration after thirty days.

Lifecycle expiration is managed by S3 and does not require the pipeline's IAM identity to have object deletion permission.

The raw object lifecycle rule was verified using S3 `HeadObject` metadata. AWS reported the matching expiration rule and expiration date.

Physical deletion is asynchronous and may happen after the recorded expiration date.

**Current limitation:** Curated data has a 30-day retention policy. Longer-term historical trend analysis will require an extended retention period.

## 13. AWS IAM and Security

The project follows the principle of least privilege.

A dedicated IAM user named `alam_workspace` is used for development and AWS service access.

### S3 Permissions

The custom S3 permissions include:

- `s3:ListAllMyBuckets`
- `s3:ListBucket`
- `s3:GetBucketLocation`
- `s3:GetObject`
- `s3:PutObject`

Object read and write access is restricted to the required project prefixes.

The project's custom S3 policy does not grant `s3:DeleteObject`.

### AWS Glue Permissions

A separate custom policy provides permissions for the Glue Data Catalog, including database, table, metadata, and partition operations.

The console also required additional listing permissions, including:

```text
glue:ListSchemas
glue:SearchTables
```

### AWS Athena Permissions

Athena access supports SQL query execution and retrieval of query results using the configured AWS workgroup and S3 output location.

### Additional Security Practices

- Avoid root account usage for routine development.
- Configure permissions through IAM.
- Use named AWS CLI profiles.
- Do not commit AWS credentials to GitHub.
- Keep local credential files outside version control.
- Use S3 server-side encryption.

S3 object metadata confirmed AES-256 server-side encryption on the inspected raw JSON object.

## 14. AWS Glue Data Catalog

AWS Glue Data Catalog provides the metadata layer for the curated dataset.

It stores database definitions, table names, column types, and S3 data locations.

### Database Configuration

```text
Database: nyc_weather_db
Table:    weather_data_daily
Region:   us-east-2
Format:   Parquet
Location: s3://nyc-weather-pipeline-unique124/curated/
```

### Registered Schema

| Column | Glue Data Type |
|---|---|
| `borough` | string |
| `observation_date` | date |
| `observation_time` | timestamp |
| `temperature_2m` | double |
| `relative_humidity_2m` | bigint |
| `wind_speed_10m` | double |

The table metadata was verified using the AWS CLI.

Example:

```bash
aws glue get-table \
  --database-name nyc_weather_db \
  --name weather_data_daily \
  --region us-east-2 \
  --profile weather-dev
```

Glue Data Catalog does not store the actual weather observations. The Parquet files remain in S3.

A separate Glue ETL job is not required because Python already performs the transformation.

### Partitioning Note

S3 data is organized in date-based folders, but the initial Glue table was created without registered partition keys.

The first Athena query successfully returned data from the initial daily dataset.

A future enhancement will implement explicit date partition management or partition projection so historical daily datasets can be queried and filtered efficiently.

## 15. Amazon Athena SQL Analytics

Amazon Athena is a serverless SQL query engine that queries data stored in Amazon S3.

Athena uses the Glue Data Catalog to discover the table metadata and locate the Parquet data.

### Athena Configuration

```text
Data source: AwsDataCatalog
Database:    nyc_weather_db
Table:       weather_data_daily
Workgroup:   primary
Region:      us-east-2
```

Query results are stored separately from curated weather data:

```text
s3://nyc-weather-pipeline-unique124/athena-results/
```

### First Successful Query

```sql
SELECT *
FROM "AwsDataCatalog"."nyc_weather_db"."weather_data_daily"
LIMIT 10;
```

**Result:** Successfully returned five rows representing the five NYC boroughs.

The initial query completed in approximately 560 milliseconds of runtime and reported approximately 0.65 KB of data scanned.

### Example 1 — Highest Temperature

```sql
SELECT
    borough,
    temperature_2m
FROM nyc_weather_db.weather_data_daily
ORDER BY temperature_2m DESC
LIMIT 1;
```

Initial result:

```text
Queens | 18.3
```

Queens recorded the highest temperature in the October 29 sample.

### Example 2 — Daily Weather Summary

```sql
SELECT
    observation_date,
    AVG(temperature_2m) AS avg_temp,
    MAX(temperature_2m) AS max_temp,
    MIN(temperature_2m) AS min_temp
FROM nyc_weather_db.weather_data_daily
GROUP BY observation_date
ORDER BY observation_date DESC;
```

For October 29, 2026:

| Date | Average Temp | Maximum Temp | Minimum Temp |
|---|---:|---:|---:|
| 2026-10-29 | 17.66 | 18.3 | 16.6 |

### Example 3 — Average Humidity by Borough

```sql
SELECT
    borough,
    AVG(relative_humidity_2m) AS avg_humidity
FROM nyc_weather_db.weather_data_daily
GROUP BY borough
ORDER BY avg_humidity DESC;
```

These queries demonstrate the serving layer for downstream reporting.

## 16. Project Setup

### Prerequisites

- Python 3.12
- AWS account
- AWS CLI
- Configured IAM credentials
- Amazon S3 bucket
- Git
- GitHub
- Required Python dependencies

### Clone Repository

```bash
git clone https://github.com/Mahmudul94/aws-serverless-weather-pipeline.git

cd aws-serverless-weather-pipeline
```

### Create a Virtual Environment

```bash
python -m venv .venv
```

On Windows PowerShell:

```powershell
.\.venv\Scripts\Activate.ps1
```

### Install Dependencies

```bash
python -m pip install -r requirements.txt
```

Core dependencies include:

```text
boto3
botocore[crt]
requests
pandas
pyarrow
```

### AWS Authentication

Configure a named AWS CLI profile with authorized credentials.

The development environment uses:

```text
weather-dev
```

Verify the active identity:

```bash
aws sts get-caller-identity --profile weather-dev
```

Verify S3 access:

```bash
aws s3 ls s3://nyc-weather-pipeline-unique124/ \
  --profile weather-dev \
  --region us-east-2
```

Credentials and sensitive configuration must never be committed to the repository.

### Run the Pipeline

The local development workflow uses separate Python scripts for extraction, S3 ingestion, and transformation.

Example project scripts:

```text
extract_weather.py
send_data_s3.py
transform_weather_data.py
```

Run the scripts in the appropriate order for your local implementation.

```bash
python extract_weather.py
python send_data_s3.py
python transform_weather_data.py
```

The data extraction and transformation stages must use matching observation dates.

For the current implementation, the daily snapshot's NYC local date identifies the S3 data partition.

**Note:** Update AWS profile, bucket, region, and configuration values as needed for your own deployment.

## 17. Cost Management

This project uses serverless AWS services to keep infrastructure overhead low.

Cost considerations include:

- Amazon S3 storage and requests.
- Amazon Athena data scanning.
- AWS Glue Data Catalog usage beyond applicable free allowances.
- Athena query result storage.
- Future AWS Lambda execution and CloudWatch logging.

Parquet and Snappy compression help reduce storage consumption and data scanned by queries.

S3 lifecycle retention limits the lifetime of raw and curated objects.

Actual charges depend on service usage and current AWS pricing.

## 18. Completed Project Milestones

| Milestone | Status |
|---|---|
| Weather REST API extraction | Completed |
| Five NYC borough ingestion | Completed |
| Raw JSON upload to S3 | Completed |
| Date-based S3 organization | Completed |
| S3 raw upload idempotency | Completed |
| Nested JSON transformation | Completed |
| Structured Pandas DataFrame | Completed |
| Missing value validation | Completed |
| Date and timestamp handling | Completed |
| Apache Parquet conversion | Completed |
| Curated Parquet upload | Completed |
| S3 curated conditional write | Completed |
| Parquet read-back verification | Completed |
| S3 lifecycle retention rules | Configured |
| IAM least-privilege policies | Configured for current services |
| Glue database and table metadata | Completed |
| Athena SQL query | Completed |

## 19. Future Improvements

The next development phases will focus on production readiness and automation.

### Data Quality

Implement additional automated checks for:

- Daily row counts.
- Uniqueness.
- Humidity and wind speed ranges.
- Timestamp consistency.
- Required borough coverage.
- Schema validation.

### Partition Management

Implement proper Glue/Athena partition management so new daily data becomes available without manual table updates.

### Serverless Automation

Deploy the Python pipeline to AWS Lambda and configure Amazon EventBridge for scheduled execution.

### CI/CD

Use GitHub Actions to automate deployment of tested pipeline code to AWS.

### Monitoring and DataOps

Add:

- Python logging.
- Exception handling.
- AWS CloudWatch monitoring.
- Pipeline execution metrics.
- Failure notifications.
- Data freshness monitoring.

### Business Intelligence

Connect Athena to Power BI or Tableau to visualize:

- Temperature comparisons by borough.
- Daily weather trends.
- Humidity trends.
- Wind speed changes.
- Historical temperature averages.

### Data Retention Improvements

Extend curated data retention when longer-term historical analytics becomes a requirement.

## 20. Data Engineering Concepts Demonstrated

This project applies the following core concepts from the data engineering lifecycle:

**Data Generation:** Weather observations generated by the Open-Meteo API.

**Data Ingestion:** Python-based REST API extraction and S3 raw storage.

**Data Storage:** Layered S3 data lake using JSON and Parquet.

**Data Transformation:** Nested JSON flattening, type conversion, and DataFrame processing.

**Data Serving:** AWS Glue metadata and Athena SQL analytics.

The project also introduces cross-cutting data engineering practices:

- Security and least privilege
- Data management
- Data lifecycle governance
- Data quality
- Data architecture
- Idempotency
- Software engineering
- DataOps and observability planning

## 21. Key Lessons Learned

Building the pipeline provided practical experience with:

- Configuring AWS CLI profiles and resolving environment issues.
- Troubleshooting IAM permissions and AWS Console access.
- Understanding S3 object keys and case sensitivity.
- Reading objects through Boto3 streaming responses.
- Managing nested JSON structures.
- Creating structured Pandas DataFrames.
- Working with timezone-aware timestamps.
- Using Parquet for analytical data storage.
- Preventing accidental object overwrites.
- Configuring and inspecting S3 lifecycle expiration.
- Registering data lake metadata with AWS Glue.
- Running SQL queries directly against S3 data using Athena.
- Understanding the difference between data storage and metadata catalogs.

## 22. References

- [Open-Meteo API Documentation](https://open-meteo.com/en/docs)
- [Amazon S3 Documentation](https://docs.aws.amazon.com/s3/)
- [Boto3 Documentation](https://boto3.amazonaws.com/v1/documentation/api/latest/index.html)
- [Pandas Documentation](https://pandas.pydata.org/docs/)
- [Apache Parquet](https://parquet.apache.org/)
- [AWS Glue Documentation](https://docs.aws.amazon.com/glue/)
- [Amazon Athena Documentation](https://docs.aws.amazon.com/athena/)
- [AWS IAM Best Practices](https://docs.aws.amazon.com/IAM/latest/UserGuide/best-practices.html)

---

**Project Status:** Core batch ingestion, transformation, S3 storage, Glue catalog integration, and Athena querying are operational. Automation, comprehensive validation, monitoring, and dashboard development are planned.

**Author:** Mahmudul Alam

**Repository:** https://github.com/Mahmudul94/aws-serverless-weather-pipeline

