# Week 4

## Setting up env variable

```bash
 export GOOGLE_APPLICATION_CREDENTIALS=./creds/service-account.json
 ```

## Load data using python script

For the EL part, I used a Python script, web_to_bq.py, utilizing dlt to load data from a website into BigQuery.

Updated URL from the website:
https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_{year}-{month}.parquet

## Data transformation and visualising data 
For the T part, I used dbt Cloud and Google Data Studio to visualize the data.