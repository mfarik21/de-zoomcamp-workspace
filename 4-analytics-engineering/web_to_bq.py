import io
import os

import dlt
import pandas as pd
import requests

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "creds/service-account.json"

fhv_tripdata_schema = {
    "dispatching_base_num": "string",
    "pickup_datetime": "datetime64[s]",
    "dropOff_datetime": "datetime64[s]",
    "PUlocationID": "Int64",
    "DOlocationID": "Int64",
    "SR_Flag": "string",
    "Affiliated_base_number": "string",
}

fhv_tripdata_column_names = {
    "dispatching_base_num": "dispatching_base_num",
    "pickup_datetime": "pickup_datetime",
    "dropOff_datetime": "dropoff_datetime",
    "PUlocationID": "pu_location_id",
    "DOlocationID": "do_location_id",
    "SR_Flag": "sr_flag",
    "Affiliated_base_number": "affiliated_base_number",
}


base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data"


def fetch_tripdata(year, service, schema=None, column_names=None):
    for i in range(12):

        # sets the month part of the file_name string
        month = "0" + str(i + 1)
        month = month[-2:]

        file_name = f"{service}_tripdata_{year}-{month}.parquet"

        # download it using requests via a pandas df
        url = f"{base_url}/{file_name}"

        print(f"Downloading {url}")

        response = requests.get(url)
        response.raise_for_status()

        parquet_file = io.BytesIO(response.content)

        df = pd.read_parquet(parquet_file)

        if schema:
            for column, dtype in schema.items():
                df[column] = df[column].astype(dtype)

        if column_names:
            df.rename(columns=column_names, inplace=True)

        yield df


pipeline = dlt.pipeline(
    pipeline_name="web_to_bq",
    destination=dlt.destinations.bigquery,
    dataset_name="trip_data_all",
)


# pipeline.run(
#     fetch_tripdata(2019, "fhv", fhv_tripdata_schema, fhv_tripdata_column_names),
#     table_name="stg_fhv_tripdata",
#     write_disposition="append",
# )

pipeline.run(
    fetch_tripdata(2020, "green"),
    table_name="green_tripdata",
    write_disposition="append",
)
