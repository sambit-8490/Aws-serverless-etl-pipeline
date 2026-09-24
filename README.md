AWS SERVERLESS
Event-Driven ETL Pipeline
S3  →  Lambda  →  S3 (Parquet Data Lake)  →  Glue Crawler  →  Athena
End-to-End Build Guide, Lambda Code Walkthrough & Troubleshooting Reference
Project type: 100% serverless · No prerequisites · Fully automated on file upload

Contents
    • 1. Project Overview
    • 2. Architecture Diagram
    • 3. How the Pipeline Works, End to End
    • 4. Prerequisites
    • 5. Step 1 — Create Your AWS Account
    • 6. Step 2 — Create the S3 Bucket and Folders
    • 7. Step 3 — Understand the Source JSON
    • 8. Step 4 — Prototype the Flatten Logic Locally
    • 9. Step 5 — Create the Lambda Function
    • 10. Step 6 — Give Lambda Permission to Read S3
    • 11. Step 7 — Add the S3 Upload Trigger
    • 12. Step 8 — Write the Full Lambda Handler
    • 13. Step 9 — Add the AWS SDK Pandas Layer
    • 14. Step 10 — Increase the Lambda Timeout
    • 15. Step 11 — Create the Glue Database and Crawler
    • 16. Step 12 — Let Lambda Auto-Trigger the Crawler
    • 17. Step 13 — Query the Data in Amazon Athena
    • 18. End-to-End Test Checklist
    • 19. Troubleshooting Log (Real Errors Hit While Building This)
    • 20. Cost Notes
    • 21. Appendix — Final, Complete Lambda Code
    • 22. Suggested Next Steps / Extensions

1. Project Overview
This document walks through building a fully event-driven, serverless ETL (Extract, Transform, Load) pipeline on AWS. "Event-driven" means there is no schedule, no cron job, and no polling — the moment a JSON file lands in an S3 bucket, the entire pipeline runs automatically: the file is flattened from nested JSON into tabular data, converted to Parquet, cataloged, and made instantly queryable in Amazon Athena.
The source data is order/transaction data in JSON format — the kind of export you might get from a NoSQL database or an e-commerce order system. Each order can contain multiple products, so the JSON is nested and needs to be "flattened" into rows before it's useful for analytics.
Why this project matters
It touches five of the most commonly used AWS data engineering services — S3, Lambda, IAM, Glue, and Athena — using only serverless, pay-per-use resources. There is no EC2 instance to manage and, for a small dataset like this, the running cost is a few rupees (well under $1) even outside the AWS Free Tier.
2. Architecture Diagram

Complete event-driven data pipeline: an S3 upload triggers Lambda, Lambda writes Parquet to a second S3 location (the data lake) and triggers a Glue Crawler, and Athena queries the resulting catalog directly — no separate database and no data duplication.
3. How the Pipeline Works, End to End
    1. A JSON file (e.g. orders_ETL.json) is uploaded into the S3 "incoming" folder — orders_json_incoming/.
    2. That upload event automatically invokes an AWS Lambda function (no scheduling required).
    3. Lambda reads the JSON from S3 using boto3, flattens the nested order → products structure into a flat list of rows using pandas, and converts it into a DataFrame.
    4. Lambda writes the DataFrame to an in-memory buffer as Parquet and uploads it to a second S3 folder — orders_parquet_datalake/ — with a timestamped filename so files never overwrite each other.
    5. Lambda immediately triggers an AWS Glue Crawler, which scans the Parquet folder and updates the Data Catalog (table schema, column types, file locations) — without copying or duplicating any data.
    6. Amazon Athena queries the Glue Data Catalog table directly, using standard SQL, for instant analysis of the newly arrived data.
The whole chain — upload → flatten → store as Parquet → catalog → query — happens with zero manual intervention after the initial setup.
4. Prerequisites
None required in terms of prior AWS experience. You will need:
    • An AWS account (a credit card is required to sign up, but this project costs at most a few rupees, and most services used are free-tier eligible for the first 12 months).
    • Python installed locally, to prototype the JSON-flattening logic before moving it into Lambda.
    • Jupyter Notebook (installed via pip install notebook) for local prototyping.
    • A sample orders JSON file (nested: a list of orders, each with a customer object and a list of products).
5. Step 1 — Create Your AWS Account
    7. Go to aws.amazon.com and choose Create an AWS Account.
    8. Enter billing details. This project will not incur meaningful charges — most services used are free for the first year, and afterward the total cost for a small dataset like this is typically under ₹10 (a few cents).
    9. Once the account is created, sign in at the AWS Management Console. On first login you can use the account's root user, or set up an IAM user (recommended long-term, but not required to complete this project).
    10. After signing in, you land on the AWS Console home page, which lists your recently used services and a search bar to jump to any service (S3, Lambda, Glue, Athena, IAM, etc.).
6. Step 2 — Create the S3 Bucket and Folders
Amazon S3 (Simple Storage Service) is scalable object storage — conceptually, unlimited hard-drive space on the internet, organized into buckets (containers) and folders inside them.
    11. Open the S3 console and choose Create bucket.
    12. S3 bucket names are globally unique across all AWS accounts, not just your own — so pick a distinctive name, e.g. my-namaste-etl-project. Leave the remaining settings at their defaults and choose Create bucket.
    13. Inside the new bucket, create two folders:
    • orders_json_incoming/ — where raw JSON transaction files will be uploaded.
    • orders_parquet_datalake/ — where Lambda will write the transformed Parquet files (your "data lake"). In production you might use separate buckets for raw vs. processed data; a single bucket with two folders keeps this project simple.
7. Step 3 — Understand the Source JSON
The source file is a JSON array. Each element in the array is one order, and each order contains order-level fields, a nested customer object, and a nested list of products (since a single order can include more than one product):
[
  {
    "order_id": "1",
    "order_date": "2024-01-05",
    "total_amount": 149.50,
    "customer": {
      "customer_id": "C001",
      "name": "Jane Doe",
      "email": "jane@example.com",
      "address": "12 Elm Street"
    },
    "products": [
      { "product_id": "P01", "name": "Wireless Mouse", "category": "Electronics", "price": 25.0, "quantity": 2 },
      { "product_id": "P02", "name": "USB Cable",      "category": "Electronics", "price": 9.5,  "quantity": 1 }
    ]
  }
]
Because a single order can contain multiple products, the flattened output will have more rows than there are orders — one row per (order, product) pair. In the demo dataset used for this project, roughly 12 orders expand into 23 flattened rows.
8. Step 4 — Prototype the Flatten Logic Locally
Before writing any Lambda code, it's worth proving the transformation logic locally in Jupyter Notebook:
    14. Confirm Python is installed: python --version.
    15. Install Jupyter: pip install notebook, then launch it with python -m notebook.
    16. Create a new notebook (e.g. flatten_data.ipynb) alongside your sample orders_ETL.json file.
    17. Read and parse the file, then flatten it into a pandas DataFrame:
import json
import pandas as pd
 
with open('orders_ETL.json', 'r') as f:
    data = json.load(f)
 
def flatten(data):
    orders_data = []
    for order in data:
        for product in order['products']:
            row = {
                "order_id": order["order_id"],
                "order_date": order["order_date"],
                "total_amount": order["total_amount"],
                "customer_id": order["customer"]["customer_id"],
                "customer_name": order["customer"]["name"],
                "email": order["customer"]["email"],
                "address": order["customer"]["address"],
                "product_id": product["product_id"],
                "product_name": product["name"],
                "category": product["category"],
                "price": product["price"],
                "quantity": product["quantity"],
            }
            orders_data.append(row)
    return pd.DataFrame(orders_data)
 
df = flatten(data)
df.head()
Once this returns a clean tabular DataFrame (12 columns, one row per order-product pair), the same function can be dropped into Lambda unchanged.
9. Step 5 — Create the Lambda Function
AWS Lambda is serverless compute: you provide the code, AWS runs it on demand and allocates capacity automatically, with no server to provision or manage.
    18. Open the Lambda console and choose Create function.
    19. Choose Author from scratch, give it a name (e.g. my_etl_pipeline), and select the Python runtime.
    20. Under Execution role, choose Create a new role with basic Lambda permissions — this creates an IAM role that Lambda will use to act on your behalf. Choose Create function.
Every Lambda function runs under an IAM execution role. By default, that role can only write its own logs — it cannot yet read from S3, which is why the next step is necessary.
10. Step 6 — Give Lambda Permission to Read S3
    21. Open the Lambda function's Configuration tab → Permissions, and click through to its execution role in the IAM console.
    22. Choose Attach policies, and attach AmazonS3FullAccess (or, for tighter security, a custom policy scoped to just your specific bucket).
Security note
AmazonS3FullAccess is used here for simplicity and to match the walkthrough. For anything beyond a learning project, scope the policy down to s3:GetObject / s3:PutObject on the exact bucket ARN instead of granting account-wide S3 access.
11. Step 7 — Add the S3 Upload Trigger
This is what makes the pipeline event-driven: Lambda needs to be wired to fire automatically whenever a new file lands in S3.
    23. In the Lambda console, choose Add trigger, and select S3 as the source.
    24. Choose your bucket, and set the event type to All object create events (or specifically PUT/POST).
    25. Set Prefix to orders_json_incoming/ so the trigger only fires for uploads to the incoming folder — not for the Parquet files Lambda itself will later write to the data-lake folder (which would otherwise cause an infinite trigger loop).
    26. Set Suffix to .json so only JSON uploads trigger the function. Acknowledge the recursive-invocation warning and choose Add.
Why the prefix/suffix filters matter
Without a prefix scoped to the incoming folder, Lambda writing its own Parquet output back into the same bucket could re-trigger itself. The prefix and .json suffix filters keep the trigger scoped to only the raw input files.
To verify the trigger works before writing any real logic, deploy a Lambda that just does print(event), upload a test file, and check CloudWatch Logs (Lambda → Monitor → View CloudWatch logs) for a new log stream. The event payload contains the bucket name and object key that caused the trigger — this is how Lambda knows which file to process, without ever hardcoding a filename:
bucket_name = event['Records'][0]['s3']['bucket']['name']
key         = event['Records'][0]['s3']['object']['key']
12. Step 8 — Write the Full Lambda Handler
With the trigger confirmed working, the full handler reads the file, flattens it, converts it to Parquet, and writes it back to S3. (The complete, final version — including the Glue Crawler trigger from Step 12 — is in the Appendix.)
import json
import boto3
import pandas as pd
import io
from datetime import datetime
 
def lambda_handler(event, context):
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
 
    s3 = boto3.client('s3')
    response = s3.get_object(Bucket=bucket_name, Key=key)
 
    # Bytes -> string -> parsed JSON
    content = response['Body'].read().decode('utf-8')
    data = json.loads(content)
 
    df = flatten(data)
 
    # Write DataFrame to an in-memory Parquet buffer (no disk write needed)
    parquet_buffer = io.BytesIO()
    df.to_parquet(parquet_buffer, index=False, engine='pyarrow')
 
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    key_staging = f'orders_parquet_datalake/orders_ETL_{timestamp}.parquet'
 
    s3.put_object(Bucket=bucket_name, Key=key_staging, Body=parquet_buffer.getvalue())
Two details worth noting:
    • index=False on to_parquet() drops the pandas row index (0, 1, 2 …) — it's not part of the actual data and shouldn't be persisted.
    • The timestamp in the output filename prevents each new upload from overwriting the previous Parquet file — this is what allows incremental loads to accumulate in the data lake.
13. Step 9 — Add the AWS SDK Pandas Layer
pandas is not included in the default Lambda Python runtime, so import pandas as pd will fail with a "module not found" error unless a layer is attached.
    27. In the Lambda console, scroll to Layers → Add a layer.
    28. Choose AWS layers, select AWS SDK for pandas (the exact name may vary slightly by runtime version), pick the latest compatible version, and choose Add.
This layer bundles pandas and pyarrow (needed for Parquet support) without you having to build and upload a custom deployment package.
14. Step 10 — Increase the Lambda Timeout
Common gotcha
By default, a new Lambda function times out after 3 seconds. Reading from S3, parsing JSON, and writing Parquet will not reliably finish in 3 seconds, so the function will appear to fail with no clear error until you look closely.
    29. Go to Configuration → General configuration → Edit.
    30. Increase Timeout to 30 seconds (comfortably more than this workload needs) and save.
15. Step 11 — Create the Glue Database and Crawler
AWS Glue is a serverless data-catalog and integration service. A Glue Crawler scans data in S3 and records its schema (column names, data types, file format, location) as metadata in the Glue Data Catalog. Crucially, the catalog stores only metadata — the actual data always stays in S3; nothing is duplicated.
    31. Open the Glue console → Data Catalog → Databases, and create a new database (e.g. etl_pipeline).
    32. Go to Crawlers → Create crawler, and give it a name (e.g. pipeline_crawler).
    33. Under Data source, choose Add a data source, browse to your S3 bucket, and select the orders_parquet_datalake/ folder.
    34. Under IAM role, choose Create new IAM role (or reuse an existing one) so the crawler has permission to read the S3 folder.
    35. Set the target database to the one just created, leave the table prefix blank, set the frequency to On demand (no schedule needed — this pipeline is fully event-driven), and choose Create crawler.
    36. Run the crawler once manually to confirm it works. After roughly a minute, check Tables in the Glue Data Catalog — a new table appears, automatically named after the S3 folder it crawled (e.g. orders_parquet_datalake), with columns and data types inferred directly from the Parquet file.
16. Step 12 — Let Lambda Auto-Trigger the Crawler
Running the crawler manually after every upload defeats the purpose of an event-driven pipeline. Instead, Lambda triggers the crawler itself immediately after writing each new Parquet file, using the Glue boto3 client:
crawler_name = 'pipeline_crawler'
glue = boto3.client('glue')
glue.start_crawler(Name=crawler_name)
This call needs a permission that a plain S3-access role does not have by default:
    37. Open the Lambda function's execution role in IAM (Configuration → Permissions → role name).
    38. Attach a policy that allows starting the crawler — for a quick start, an AWS managed Glue service role/policy works; for production, scope it to glue:StartCrawler on the specific crawler's ARN.
With this in place, every new file upload now triggers, end to end and without any manual step: Lambda run → Parquet write → Glue Crawler run → updated Athena table.
17. Step 13 — Query the Data in Amazon Athena
Amazon Athena is a serverless, interactive SQL query service that reads directly from the Glue Data Catalog (and therefore directly from S3) — there is no database to provision and no data to load.
    39. Open the Athena console → Query editor.
    40. Under Data source, choose AwsDataCatalog, and under Database, choose the database created in Step 11 (e.g. etl_pipeline).
    41. Run a basic query against the crawled table:
SELECT *
FROM "etl_pipeline"."orders_parquet_datalake"
LIMIT 10;
    42. Run an aggregation, e.g. total sales per customer:
SELECT customer_id, SUM(total_amount) AS total_sales
FROM "etl_pipeline"."orders_parquet_datalake"
GROUP BY customer_id
ORDER BY total_sales DESC;
Every subsequent file uploaded to the incoming/ folder flows through the same pipeline automatically, and its rows become queryable in Athena within roughly a minute (the time it takes Lambda to run and the crawler to re-scan).
18. End-to-End Test Checklist
    • Upload orders_ETL.json to orders_json_incoming/ in S3.
    • Check CloudWatch Logs for the Lambda function — confirm no errors and that the flattened DataFrame shape looks right (e.g. "23 rows, 12 columns").
    • Check orders_parquet_datalake/ in S3 — a new, timestamped .parquet file should appear.
    • Check the Glue console — the crawler's most recent run should show as Succeeded.
    • Run SELECT COUNT(*) in Athena — the row count should match the newly flattened data.
    • Upload a second, smaller incremental JSON file with one or two new orders, and re-run the same checklist — the Athena row count should increase by exactly the number of new order-product rows, with no duplicates and no overwritten files.
19. Troubleshooting Log (Real Errors Hit While Building This)
Symptom
Cause & Fix
Task timed out after 3.00 seconds
Default Lambda timeout is 3 seconds. Fix: Configuration → General configuration → Edit → increase Timeout to 30 seconds (Step 10).
Unable to import module 'lambda_function': No module named 'pandas'
pandas isn't bundled with the Lambda Python runtime. Fix: attach the AWS SDK for pandas layer (Step 9).
module 'datetime' has no attribute 'now'
Caused by import datetime and then calling datetime.now() — the module, not the class, was imported. Fix: use from datetime import datetime, then datetime.now().
Two Parquet files appear for a single upload
Lambda retried automatically after an earlier invocation failed (e.g. due to the timeout or import error above). Once the underlying error is fixed, delete the duplicate/partial files and re-upload — a single successful run produces exactly one file.
AccessDenied when Lambda calls s3.get_object
The Lambda execution role has no S3 permissions yet. Fix: attach an S3 read (or S3 full access) policy to the role (Step 6).
AccessDenied when Lambda calls glue.start_crawler
The execution role can read S3 but has no Glue permissions. Fix: attach a Glue service policy scoped to StartCrawler (Step 12).
Athena shows 0 rows immediately after upload
The crawler hasn't finished running yet — there's a short lag between the Parquet file landing and the catalog updating. Fix: wait a few seconds and re-run the query, or check the crawler's run status in the Glue console.
20. Cost Notes
For a small dataset like the one used in this project (a handful of KB-sized JSON files), the running cost is negligible:
    • S3 storage and requests: fractions of a cent for this data volume.
    • Lambda: within the AWS Free Tier's 1M free requests / month for the first year, and effectively free at this scale afterward.
    • Glue Crawler: billed per DPU-hour of crawling time; a crawl of a tiny folder like this completes in about a minute.
    • Athena: billed per data scanned per query; a few small Parquet files cost a fraction of a cent per query.
Overall, expect a total cost of well under ₹10 (roughly a few US cents) even for extended experimentation, and effectively ₹0 for the first 12 months on a new account under the Free Tier.
21. Appendix — Final, Complete Lambda Code
This is the finished lambda_function.py, combining every step above: read from S3, flatten JSON, write Parquet with a timestamped key, and auto-trigger the Glue Crawler.
import json
import boto3
import pandas as pd
import io
from datetime import datetime
 
def flatten(data):
    orders_data = []
    for order in data:
        for product in order['products']:
            row_orders = {
                "order_id": order["order_id"],
                "order_date": order["order_date"],
                "total_amount": order["total_amount"],
                "customer_id": order["customer"]["customer_id"],
                "customer_name": order["customer"]["name"],
                "email": order["customer"]["email"],
                "address": order["customer"]["address"],
                "product_id": product["product_id"],
                "product_name": product["name"],
                "category": product["category"],
                "price": product["price"],
                "quantity": product["quantity"]
            }
            orders_data.append(row_orders)
    df_orders = pd.DataFrame(orders_data)
    return df_orders
 
def lambda_handler(event, context):
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
 
    s3 = boto3.client('s3')
    response = s3.get_object(Bucket=bucket_name, Key=key)
 
    # Read and parse JSON content
    content = response['Body'].read().decode('utf-8')
    data = json.loads(content)
    df = flatten(data)
 
    # In-memory binary buffer to hold data like a file, without writing to disk
    parquet_buffer = io.BytesIO()
    df.to_parquet(parquet_buffer, index=False, engine='pyarrow')
 
    now = datetime.now()
    timestamp = now.strftime('%Y%m%d_%H%M%S')
 
    key_staging = f'orders_parquet_datalake/orders_ETL_{timestamp}.parquet'
 
    s3.put_object(Bucket=bucket_name, Key=key_staging, Body=parquet_buffer.getvalue())
 
    crawler_name = 'etl_pipeline_crawler'
    glue = boto3.client('glue')
    response = glue.start_crawler(Name=crawler_name)
22. Suggested Next Steps / Extensions
    • Scope IAM policies down from *FullAccess to least-privilege, resource-specific permissions once the pipeline works end to end.
    • Add error handling and a Dead Letter Queue (DLQ) or SNS alert for malformed JSON files.
    • Partition the Parquet data lake by date (e.g. year=/month=/day=/) to keep Athena queries fast and cheap as data volume grows.
    • Add a Glue Crawler schedule as a fallback safety net, in addition to the event-driven trigger from Lambda.
    • Replace print-based debugging with structured logging (the logging module) for easier CloudWatch filtering.
    • Add a data quality check step (e.g. AWS Glue Data Quality or a simple pandas validation) before writing to the data lake.
    • Visualize the Athena tables in Amazon QuickSight for a lightweight BI layer on top of this pipeline.
