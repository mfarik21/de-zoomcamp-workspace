import pyarrow as pa
import pyarrow.parquet as pq
import os

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_data(data, *args, **kwargs):
    table = pa.Table.from_pandas(data)
    pq.write_table(table, 'nyc_taxi_data.parquet')

