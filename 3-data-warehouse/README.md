# Week 3


## Homework

### Setting up env variable

```bash
 export GOOGLE_APPLICATION_CREDENTIALS=./creds/service-account.json
 ```

 ```bash
 export GCP_GCS_BUCKET=de-zoomcamp-mfarik
 ```

 ### Load data using python script

Updated URL from website:

https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-01.parquet


Updated load script to load parquet data from web to GCS:
```python
import os
import requests
from google.cloud import storage

init_url = "https://d37ci6vzurychx.cloudfront.net/trip-data"
BUCKET = os.environ.get("GCP_GCS_BUCKET", "dtc-data-lake-bucketname")


def upload_to_gcs(bucket, object_name, local_file):
    client = storage.Client()
    bucket = client.bucket(bucket)
    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)


def web_to_gcs(year, service):
    for i in range(12):

        # sets the month part of the file_name string
        month = "0" + str(i + 1)
        month = month[-2:]

        # csv file_name
        file_name = f"{service}_tripdata_{year}-{month}.parquet"

        # download it using requests via a pandas df
        request_url = f"{init_url}/{file_name}"

        r = requests.get(request_url)
        open(f"data/{file_name}", "wb").write(r.content)
        print(f"Local: {file_name}")

        # upload it to gcs
        upload_to_gcs(BUCKET, f"{service}/{file_name}", file_name)
        print(f"GCS: {service}/{file_name}")


web_to_gcs("2022", "green")

```

## BigQuery

### Create an external table from Green Taxi Trip Records Data for 2022:

```sql
CREATE OR REPLACE EXTERNAL TABLE `de-zoomcamp-412513.nytaxi.external_green_tripdata`
OPTIONS (
  format = 'parquet',
  uris = ['gs://de-zoomcamp-mfarik/green/green_tripdata_2022-*.parquet']
);
```

### Create a table from Green Taxi Trip Records Data for 2022:

```sql
CREATE OR REPLACE TABLE `de-zoomcamp-412513.nytaxi.green_tripdata`
AS SELECT * FROM `de-zoomcamp-412513.nytaxi.external_green_tripdata`;
```

### Create a partitioned 

```sql
CREATE OR REPLACE TABLE `de-zoomcamp-412513.nytaxi.green_tripdata_partitioned`
PARTITION BY DATE(lpep_pickup_datetime)
CLUSTER BY PUlocationID AS (
  SELECT * FROM `ny_taxi.green_tripdata`
);
```

