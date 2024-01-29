### Running Postgres on Docker
```bash
docker run -it \
  -e POSTGRES_USER="root" \
  -e POSTGRES_PASSWORD="root" \
  -e POSTGRES_DB="ny_taxi" \
  -v $(pwd)ny_taxi_postgres_data:/var/lib/postgresql/data \
  -p 5432:5432 \
  postgres:13
```

### Cnnnect to the Database using pgcli 
```bash
pip install pgcli

pgcli -h localhost -p 5432 -u root -d ny_taxi

```

### Run pgadmin
```bash

docker run -it \
    -e PGADMIN_DEFAULT_EMAIL="admin@admin.com" \
    -e PGADMIN_DEFAULT_PASSWORD="root" \
    -p 8080:80 \
    dpage/pgadmin4
```

### Create a Network in Docker
```bash
docker network create pg-network

docker run -it \
  -e POSTGRES_USER="root" \
  -e POSTGRES_PASSWORD="root" \
  -e POSTGRES_DB="ny_taxi" \
  -v $(pwd)ny_taxi_postgres_data:/var/lib/postgresql/data \
  -p 5432:5432 \
  --network=pg-network \
  --name pg-database \
  postgres:13


docker run -it \
    -e PGADMIN_DEFAULT_EMAIL="admin@admin.com" \
    -e PGADMIN_DEFAULT_PASSWORD="root" \
    -p 8080:80 \
    --network=pg-network \
    dpage/pgadmin4
```

### Convert Notebook into Script
```bash
jupyter nbconvert --to=script upload-data.ipynb
```

### Ingest Data Using Script
```bash
URL="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz"
python ingest_data.py \
--user=root \
--password=root \
--host=localhost \
--port=5432 \
--db=ny_taxi \
--table_name=yellow_taxi_trips \
--url=${URL}
```

### Build Image for Ingesting Data
```bash
docker build -t taxi_ingest:v001 .

docker run -it --network=pg-network \
taxi_ingest:v001 \
--user=root \
--password=root \
--host=localhost \
--port=5432 \
--db=ny_taxi \
--table_name=yellow_taxi_trips \
--url=${URL}
```

# Module 1 Homework

## Docker & SQL

### Question 1. Knowing docker tags
Run the command to get information on Docker

docker --help

Now run the command to get help on the "docker build" command:

docker build --help

Do the same for "docker run".

Which tag has the following text? - Automatically remove the container when it exits

```bash
    docker run --rm IMAGE
```

### Question 2. Understanding docker first run
Run docker with the python:3.9 image in an interactive mode and the entrypoint of bash. Now check the python modules that are installed ( use pip list ).

What is version of the package wheel ?

```bash
docker run -it --entrypoint=bash python:3.9
root@ae4bc0e58532:/# pip list
Package    Version
---------- -------
pip        23.0.1
setuptools 58.1.0
wheel      0.42.0

```

### Question 3. Count records
How many taxi trips were totally made on September 18th 2019?

Tip: started and finished on 2019-09-18.

Remember that lpep_pickup_datetime and lpep_dropoff_datetime columns are in the format timestamp (date and hour+min+sec) and not in date.

15612

```sql
SELECT COUNT(*) FROM green_taxi_trips
WHERE DATE(lpep_pickup_datetime) = '2019-09-18'
	AND DATE(lpep_dropoff_datetime) = '2019-09-18'
```

### Question 4. Largest trip for each day
Which was the pick up day with the largest trip distance Use the pick up time for your calculations.

2019-09-26

```sql
SELECT DATE(lpep_pickup_datetime)
FROM green_taxi_trips
WHERE trip_distance = (
    SELECT MAX(trip_distance)
    FROM green_taxi_trips
)
```

### Question 5. Three biggest pick up Boroughs
Consider lpep_pickup_datetime in '2019-09-18' and ignoring Borough has Unknown

Which were the 3 pick up Boroughs that had a sum of total_amount superior to 50000?

Borough     total_amount
"Brooklyn"	96333.23999999913
"Manhattan"	92271.29999999993
"Queens"	78671.70999999931

```sql
SELECT z."Borough", SUM(g.total_amount) AS total_amount
FROM green_taxi_trips g 
LEFT JOIN zones z ON g."PULocationID" = z."LocationID"
WHERE DATE(g.lpep_pickup_datetime) = '2019-09-18'
GROUP BY z."Borough"
HAVING SUM(g.total_amount) > 50000
ORDER BY total_amount DESC;

```


### Question 6. Largest tip

For the passengers picked up in September 2019 in the zone name Astoria which was the drop off zone that had the largest tip? We want the name of the zone, not the id.

Note: it's not a typo, it's tip , not trip

JFK Airport



```sql
SELECT z."Zone"
FROM zones z,
     (
         SELECT "PULocationID", "DOLocationID", tip_amount
         FROM green_taxi_trips
         WHERE "PULocationID" = (
                 SELECT "LocationID"
                 FROM zones
                 WHERE "Zone" = 'Astoria'
             )
           AND EXTRACT(YEAR FROM lpep_pickup_datetime) = 2019
           AND EXTRACT(MONTH FROM lpep_pickup_datetime) = 9
         ORDER BY tip_amount DESC
         LIMIT 1
     ) m
WHERE z."LocationID" = m."DOLocationID";

```

## Terraform
After updating the main.tf and variable.tf files run:

terraform apply
Paste the output of this command into the homework submission form.

```bash
Apply complete! Resources: 2 added, 0 changed, 0 destroyed.
``` 