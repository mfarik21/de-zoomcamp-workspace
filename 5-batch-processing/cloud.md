python 06_spark_sql.py \
    --input_green=data/pq/green/2020/*/ \
    --input_yellow=data/pq/yellow/2020/*/ \
    --outpu=data/report-2


URL="spark://farik-ThinkPad-E14-Gen-2:7077"

spark-submit \ 
    --master="${URL}" \
    06_spark_sql.py \
        --input_green=data/pq/green/2021/*/ \
        --input_yellow=data/pq/yellow/2021/*/ \
        --output=data/report-2021

        --input_green=gs://de-zoomcamp-mfarik/pq/green/2021/*/ \
        --input_yellow=gs://de-zoomcamp-mfarik/pq/yellow/2021/*/ \
        --output=gs://de-zoomcamp-mfarik/report-2021


https://cloud.google.com/dataproc/docs/guides/submit-job#dataproc-submit-job-gcloud

gcloud dataproc jobs submit pyspark \
    --cluster=de-zoomcamp-cluster \
    --region=us-central1 \
    gs://de-zoomcamp-mfarik/code/06_spark_sql.py \
    -- \
        --input_green=gs://de-zoomcamp-mfarik/pq/green/2020/*/ \
        --input_yellow=gs://de-zoomcamp-mfarik/pq/yellow/2020/*/ \
        --output=gs://de-zoomcamp-mfarik/report-2020


https://cloud.google.com/dataproc/docs/tutorials/bigquery-connector-spark-example

gcloud dataproc jobs submit pyspark \
    --cluster=de-zoomcamp-cluster \
    --region=us-central1 \
    --jars=gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar \
    gs://de-zoomcamp-mfarik/code/06_spark_sql_bigquery.py \
    -- \
        --input_green=gs://de-zoomcamp-mfarik/pq/green/2021/*/ \
        --input_yellow=gs://de-zoomcamp-mfarik/pq/yellow/2021/*/ \
        --output=trips_data_all.reports-2021