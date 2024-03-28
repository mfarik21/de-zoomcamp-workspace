import json
import time
import pandas as pd

from kafka import KafkaProducer


def json_serializer(data):
    return json.dumps(data).encode("utf-8")


server = "localhost:9092"

producer = KafkaProducer(bootstrap_servers=[server], value_serializer=json_serializer)

print("Server connected:", producer.bootstrap_connected())

t0 = time.time()
topic_name = "green-trips"

file_path = "../resources/green_tripdata_2019-10.csv"
df_green = pd.read_csv(file_path)

for row in df_green.itertuples(index=False):
    row_dict = {col: getattr(row, col) for col in row._fields}
    producer.send(topic_name, value=row_dict)
    print(f"Sent: {row_dict}")

t1 = time.time()
print(f"took {(t1 - t0):.2f} seconds")
