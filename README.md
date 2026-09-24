# AWS Serverless Event-Driven JSON-to-Parquet ETL Pipeline

A fully serverless AWS data-engineering project that automatically converts nested JSON order data into analytics-ready Apache Parquet files.

When a JSON file is uploaded to Amazon S3, an S3 event invokes AWS Lambda. Lambda flattens the nested customer and product data with Pandas, writes Parquet using PyArrow, stores the output in a separate S3 target prefix, starts an AWS Glue crawler, and makes the resulting table queryable with Amazon Athena.

## Architecture

```text
Nested JSON file
      |
      v
Amazon S3 data-lake bucket: source/
      |
      | S3 ObjectCreated event
      | Filter: prefix=source/, suffix=.json
      v
AWS Lambda
  - Reads JSON
  - Flattens customer/products structure
  - Converts rows to Parquet
      |
      v
Amazon S3 data-lake bucket: target/orders_parquet_datalake/
      |
      v
AWS Glue Crawler
      |
      v
AWS Glue Data Catalog
      |
      v
Amazon Athena
      |
      v
Separate Amazon S3 bucket: athena-query-results/
```

## Services used

| AWS service | Purpose |
|---|---|
| Amazon S3 | Stores raw JSON source files, processed Parquet files, and Athena query results |
| AWS Lambda | Event-driven transformation component |
| Lambda layer | Provides `pandas`, `pyarrow`, and dependencies such as NumPy |
| AWS Glue Crawler | Detects Parquet schema and creates/updates Glue Data Catalog tables |
| AWS Glue Data Catalog | Stores metadata used by Athena |
| Amazon Athena | Runs SQL queries against Parquet data in S3 |
| AWS IAM | Controls least-privilege access for Lambda, Glue, and Athena |
| Amazon CloudWatch Logs | Stores Lambda logs and errors |

## S3 layout

Use one bucket for raw and processed data, with separate prefixes, and a second bucket for Athena results.

```text
yourname-etl-data-lake/
├── source/
│   └── orders.json
└── target/
    └── orders_parquet_datalake/
        └── orders_ETL_YYYYMMDD_HHMMSS.parquet

yourname-athena-query-results/
└── athena-query-results/
    └── query-result.csv
```

> Important: Do not store Athena query-result CSV files in `target/orders_parquet_datalake/`. Athena expects all files under the Parquet table location to be Parquet files. Mixing CSV or JSON with Parquet can cause `HIVE_BAD_DATA: Expected magic number PAR1` errors.

## Prerequisites

- An AWS account with access to S3, Lambda, IAM, Glue, Athena, and CloudWatch.
- A single AWS Region for all services. Example: `ap-south-1` (Mumbai).
- A Lambda-compatible layer containing:

```text
pandas
pyarrow
numpy
```

- The Lambda runtime and layer must use the same Python version and CPU architecture.

## Resource names

Replace `yourname` with a globally unique value.

```text
Data-lake bucket:       yourname-etl-data-lake
Athena results bucket:  yourname-athena-query-results
Lambda function:        json-to-parquet-lambda
Lambda role:            json-to-parquet-lambda-role
Glue database:          etl_pipeline_db
Glue crawler:           etl_pipeline_crawler
Crawler role:           etl-pipeline-crawler-role
```

## Setup steps

### 1. Create S3 buckets

Create these buckets in the same Region:

```text
yourname-etl-data-lake
yourname-athena-query-results
```

Keep **Block all public access** enabled.

Create these prefixes:

```text
s3://yourname-etl-data-lake/source/
s3://yourname-etl-data-lake/target/orders_parquet_datalake/
s3://yourname-athena-query-results/athena-query-results/
```

### 2. Configure Athena query results

In **Amazon Athena → Settings → Manage**, configure:

```text
s3://yourname-athena-query-results/athena-query-results/
```

If your Athena workgroup overrides console settings, configure the same location in:

```text
Athena → Workgroups → primary → Edit
```

### 3. Create Lambda execution role

Create `json-to-parquet-lambda-role` with trusted entity **Lambda**.

Attach managed policy:

```text
AWSLambdaBasicExecutionRole
```

Add the following inline policy. Replace `YOUR-DATA-LAKE-BUCKET`.

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "ReadJsonFromSourceFolder",
      "Effect": "Allow",
      "Action": ["s3:GetObject"],
      "Resource": "arn:aws:s3:::YOUR-DATA-LAKE-BUCKET/source/*"
    },
    {
      "Sid": "WriteParquetToTargetFolder",
      "Effect": "Allow",
      "Action": ["s3:PutObject"],
      "Resource": "arn:aws:s3:::YOUR-DATA-LAKE-BUCKET/target/orders_parquet_datalake/*"
    },
    {
      "Sid": "StartGlueCrawler",
      "Effect": "Allow",
      "Action": ["glue:StartCrawler", "glue:GetCrawler"],
      "Resource": "*"
    }
  ]
}
```

### 4. Create or attach Lambda layer

Your code calls:

```python
import pandas as pd
...
df.to_parquet(parquet_buffer, index=False, engine="pyarrow")
```

Therefore, attach a layer containing `pandas` and `pyarrow`.

The layer ZIP structure must have `python/` at its root:

```text
layer.zip
└── python/
    ├── pandas/
    ├── pyarrow/
    ├── numpy/
    └── *.dist-info/
```

### 5. Create Lambda function

Create the function with:

| Setting | Value |
|---|---|
| Function name | `json-to-parquet-lambda` |
| Runtime | Python version matching the layer |
| Architecture | Matching the layer: `x86_64` or `arm64` |
| Execution role | `json-to-parquet-lambda-role` |
| Memory | 1024 MB recommended starting point |
| Timeout | 1 minute recommended starting point |

Attach the Pandas/PyArrow layer.

## Lambda code

```python
import json
import boto3
import pandas as pd
import io
import urllib.parse
from datetime import datetime


def flatten(data):
    orders_data = []

    if isinstance(data, dict):
        data = [data]

    for order in data:
        customer = order.get("customer", {})

        for product in order.get("products", []):
            row_orders = {
                "order_id": order.get("order_id"),
                "order_date": order.get("order_date"),
                "total_amount": order.get("total_amount"),
                "customer_id": customer.get("customer_id"),
                "customer_name": customer.get("name"),
                "email": customer.get("email"),
                "address": customer.get("address"),
                "product_id": product.get("product_id"),
                "product_name": product.get("name"),
                "category": product.get("category"),
                "price": product.get("price"),
                "quantity": product.get("quantity")
            }
            orders_data.append(row_orders)

    return pd.DataFrame(orders_data)


def lambda_handler(event, context):
    s3 = boto3.client("s3")
    glue = boto3.client("glue")
    output_files = []

    for record in event["Records"]:
        bucket_name = record["s3"]["bucket"]["name"]
        key = urllib.parse.unquote_plus(record["s3"]["object"]["key"])

        if not key.lower().endswith(".json"):
            continue

        response = s3.get_object(Bucket=bucket_name, Key=key)
        content = response["Body"].read().decode("utf-8")
        data = json.loads(content)
        df = flatten(data)

        if df.empty:
            print(f"No products found in file: {key}")
            continue

        parquet_buffer = io.BytesIO()
        df.to_parquet(parquet_buffer, index=False, engine="pyarrow")

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        key_staging = (
            "target/orders_parquet_datalake/"
            f"orders_ETL_{timestamp}.parquet"
        )

        s3.put_object(
            Bucket=bucket_name,
            Key=key_staging,
            Body=parquet_buffer.getvalue(),
            ContentType="application/octet-stream"
        )

        output_files.append(f"s3://{bucket_name}/{key_staging}")

    if output_files:
        try:
            glue.start_crawler(Name="etl_pipeline_crawler")
        except glue.exceptions.CrawlerRunningException:
            print("Glue crawler is already running.")

    return {
        "statusCode": 200,
        "message": "JSON flattened and converted to Parquet.",
        "output_files": output_files
    }
```

## Create Glue database

In **AWS Glue → Data Catalog → Databases**, create:

```text
etl_pipeline_db
```

## Create Glue crawler role

Create `etl-pipeline-crawler-role` with trusted entity **Glue**.

Attach managed policy:

```text
AWSGlueServiceRole
```

Add this inline S3 policy after replacing `YOUR-DATA-LAKE-BUCKET`.

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "ListTargetFolder",
      "Effect": "Allow",
      "Action": ["s3:ListBucket"],
      "Resource": "arn:aws:s3:::YOUR-DATA-LAKE-BUCKET",
      "Condition": {
        "StringLike": {
          "s3:prefix": ["target/orders_parquet_datalake/*"]
        }
      }
    },
    {
      "Sid": "ReadParquetFiles",
      "Effect": "Allow",
      "Action": ["s3:GetObject"],
      "Resource": "arn:aws:s3:::YOUR-DATA-LAKE-BUCKET/target/orders_parquet_datalake/*"
    }
  ]
}
```

## Create Glue crawler

Create a crawler with the following configuration:

| Setting | Value |
|---|---|
| Crawler name | `etl_pipeline_crawler` |
| Data source | Amazon S3 |
| S3 path | `s3://yourname-etl-data-lake/target/orders_parquet_datalake/` |
| IAM role | `etl-pipeline-crawler-role` |
| Target database | `etl_pipeline_db` |

Do not point the crawler at the bucket root, `source/`, or the Athena results bucket.

## Create S3 trigger

Add an S3 trigger to `json-to-parquet-lambda`.

| Setting | Value |
|---|---|
| Bucket | `yourname-etl-data-lake` |
| Event type | `s3:ObjectCreated:*` |
| Prefix | `source/` |
| Suffix | `.json` |

This avoids an infinite loop. Files written by Lambda to `target/` do not match the `source/*.json` trigger filter.

## Athena permissions

The IAM user or role used to run Athena needs permission to:

- Start/read Athena query executions.
- Read Parquet files under the target prefix.
- Read Glue Catalog metadata.
- Write and read query results in the separate Athena results bucket.

For a learning account, `AmazonAthenaFullAccess` can simplify setup, but you still need S3 write permission to the Athena results bucket.

## Test input

Upload this as `orders.json` to:

```text
s3://yourname-etl-data-lake/source/orders.json
```

```json
[
  {
    "order_id": 13,
    "order_date": "2024-01-10",
    "total_amount": 255.50,
    "customer": {
      "customer_id": 113,
      "name": "John Doe",
      "email": "johndoe@example.com",
      "address": "121 Main St, Springfield"
    },
    "products": [
      {
        "product_id": "P01",
        "name": "Wireless Mouse",
        "category": "Electronics",
        "price": 25.00,
        "quantity": 2
      },
      {
        "product_id": "P02",
        "name": "Bluetooth Keyboard",
        "category": "Electronics",
        "price": 45.00,
        "quantity": 1
      }
    ]
  }
]
```

## Expected transformed output

The nested `products` array becomes separate analytics rows:

| order_id | customer_name | product_id | product_name | price | quantity |
|---:|---|---|---|---:|---:|
| 13 | John Doe | P01 | Wireless Mouse | 25.00 | 2 |
| 13 | John Doe | P02 | Bluetooth Keyboard | 45.00 | 1 |

## Validation checklist

1. Upload JSON into `source/`.
2. Confirm Lambda runs in CloudWatch Logs.
3. Confirm a `.parquet` file appears in `target/orders_parquet_datalake/`.
4. Confirm Glue crawler status changes from `Running` to `Ready`.
5. Confirm a table appears in Glue database `etl_pipeline_db`.
6. Query the crawler-created table in Athena.
7. Confirm Athena output appears in the separate results bucket.

## Athena queries

List crawler-created tables:

```sql
SHOW TABLES;
```

Preview data. Replace `YOUR_TABLE_NAME` with the table created by the crawler:

```sql
SELECT *
FROM "AwsDataCatalog"."etl_pipeline_db"."YOUR_TABLE_NAME"
LIMIT 10;
```

Revenue by product:

```sql
SELECT
    product_name,
    SUM(quantity) AS units_sold,
    SUM(price * quantity) AS revenue
FROM "AwsDataCatalog"."etl_pipeline_db"."YOUR_TABLE_NAME"
GROUP BY product_name
ORDER BY revenue DESC;
```

Customer purchase summary:

```sql
SELECT
    customer_name,
    email,
    COUNT(DISTINCT order_id) AS order_count,
    SUM(price * quantity) AS calculated_product_total
FROM "AwsDataCatalog"."etl_pipeline_db"."YOUR_TABLE_NAME"
GROUP BY customer_name, email
ORDER BY calculated_product_total DESC;
```

Find total amount mismatches:

```sql
SELECT
    order_id,
    MAX(total_amount) AS declared_order_total,
    SUM(price * quantity) AS calculated_product_total,
    MAX(total_amount) - SUM(price * quantity) AS difference
FROM "AwsDataCatalog"."etl_pipeline_db"."YOUR_TABLE_NAME"
GROUP BY order_id
HAVING MAX(total_amount) <> SUM(price * quantity);
```

## Troubleshooting

| Problem | Likely cause | Fix |
|---|---|---|
| `No output location provided` | Athena output path is missing | Configure the separate Athena results bucket in Athena settings/workgroup |
| `Expected magic number PAR1` | CSV/JSON exists in the Parquet table location | Remove non-Parquet files from target prefix; keep Athena output separate |
| `mismatched input 'limit'` | `LIMIT` was run without `SELECT` | Use `SELECT * FROM table LIMIT 10;` |
| `No module named pandas` | Layer missing/incompatible | Attach Lambda-compatible Pandas layer matching runtime/architecture |
| `No module named pyarrow` | Layer has Pandas only | Include PyArrow in the layer |
| `AccessDenied` reading S3 | Lambda/crawler lacks `s3:GetObject` | Correct source/target IAM policies |
| `AccessDenied` writing S3 | Lambda lacks `s3:PutObject` | Permit output writes to the target prefix |
| `AccessDenied` starting crawler | Lambda lacks Glue permission | Add `glue:StartCrawler` |
| `CrawlerRunningException` | A crawler is already running | Catch the exception; wait for crawler to become Ready |
| Lambda runs repeatedly | Trigger also matches output files | Use S3 trigger filters: prefix `source/`, suffix `.json` |

## Improvements

- Add `line_total = price * quantity` in Lambda.
- Write partitioned output such as `year=YYYY/month=MM/day=DD/` for Athena cost optimization.
- Add CloudWatch alarms for Lambda errors and duration.
- Enable S3 Versioning for raw input data.
- Add SQS dead-letter queue/failure destination for failed processing events.
- Use AWS Step Functions or EventBridge for advanced orchestration.
- Use AWS Glue ETL for large files or high-volume datasets.

## Cost cleanup

When finished, remove resources you no longer need:

- Delete test objects from both S3 buckets, then delete buckets if no longer needed.
- Delete Lambda function and layer versions.
- Delete Glue crawler and optional Glue table/database.
- Review CloudWatch log retention.
- Remove unused IAM roles/policies only after confirming they are not used elsewhere.

## Resume description

> Built a serverless event-driven ETL pipeline on AWS that ingests nested JSON transaction data from Amazon S3. Configured S3 object-created events to invoke AWS Lambda, which flattened customer and product records using Pandas, converted output to Apache Parquet through PyArrow, and stored optimized files in a separate S3 target prefix. Automated schema discovery with AWS Glue Crawler and queried the cataloged dataset through Amazon Athena. Implemented separate raw, processed, and query-result storage locations, IAM least-privilege access, and CloudWatch-based troubleshooting.
