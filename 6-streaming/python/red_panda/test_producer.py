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
topic_name = "test-topic"

for i in range(10):
    message = {"number": i}
    producer.send(topic_name, value=message)
    print(f"Sent: {message}")
    time.sleep(0.05)

t1 = time.time()
print(f"took {(t1 - t0):.2f} seconds")

producer.flush()
t2 = time.time()

print(f"took {(t2 - t1):.2f} seconds")


file_path = "../resources/green_tripdata_2019-10.csv"
df_green = pd.read_csv(file_path)

for row in df_green.itertuples(index=False):
    row_dict = {col: getattr(row, col) for col in row._fields}
    print(row_dict)
    break


# TODO implement sending the data here
