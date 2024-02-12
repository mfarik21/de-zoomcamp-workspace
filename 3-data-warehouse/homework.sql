-- CREATE EXTERNAL TABLE
CREATE OR REPLACE EXTERNAL TABLE `de-zoomcamp-412513.nytaxi.external_green_tripdata`
OPTIONS (
    format = 'parquet',
    uris = ['gs://de-zoomcamp-mfarik/green/green_tripdata_2022-*.parquet']
);


-- CREATE MATERIALIZED TABLE
CREATE OR REPLACE TABLE `de-zoomcamp-412513.nytaxi.green_tripdata`
AS SELECT * FROM `de-zoomcamp-412513.nytaxi.external_green_tripdata`;

-- QUESTION 1
SELECT COUNT(*) FROM `de-zoomcamp-412513.nytaxi.green_tripdata`

-- QUESTION 2
SELECT DISTINCT PULocationID FROM `de-zoomcamp-412513.nytaxi.external_green_tripdata`
SELECT DISTINCT PULocationID FROM `de-zoomcamp-412513.nytaxi.green_tripdata`

-- QUESTION 3
SELECT DISTINCT PULocationID FROM `de-zoomcamp-412513.nytaxi.green_tripdata`

-- QUESTION 4
-- CREATE A TABLE PARTITIONED BY lpep_pickup_datetime AND CLUSTERED BY PUlocationID
CREATE OR REPLACE TABLE `de-zoomcamp-412513.nytaxi.green_tripdata_partitioned`
PARTITION BY DATE(lpep_pickup_datetime)
CLUSTER BY PUlocationID AS (
  SELECT * FROM `ny_taxi.green_tripdata`
);

-- QUESTION 5
SELECT DISTINCT PULocationID 
FROM `de-zoomcamp-412513.nytaxi.green_tripdata` 
WHERE DATE(lpep_pickup_datetime) BETWEEN DATE("2022-06-01") AND DATE("2022-06-30");

SELECT DISTINCT PULocationID 
FROM `de-zoomcamp-412513.nytaxi.green_tripdata_partitioned` 
WHERE DATE(lpep_pickup_datetime) BETWEEN DATE("2022-06-01") AND DATE("2022-06-30");