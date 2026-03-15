import pandas as pd

url = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-10.parquet"
columns = [
    'lpep_pickup_datetime',
    'lpep_dropoff_datetime',
    'PULocationID',
    'DOLocationID',
    'passenger_count',
    'trip_distance',
    'tip_amount',
    'total_amount'
]
df = pd.read_parquet(url, columns=columns)

import dataclasses
import os
from ride_utils import ride_from_row

import json
from kafka import KafkaProducer

server = 'localhost:9092'
topic_name = 'green-trips'

def ride_serializer(ride):
    ride_dict = dataclasses.asdict(ride)
    json_str = json.dumps(ride_dict)
    return json_str.encode('utf-8')

producer = KafkaProducer(
    bootstrap_servers=[server],
    value_serializer=ride_serializer
)

import time

t0 = time.time()
# sleep_seconds = float(os.getenv("PRODUCER_SLEEP_SECONDS", "0.001"))
log_every = max(1, int(os.getenv("PRODUCER_LOG_EVERY", "100")))

for idx, row in df.iterrows():
    ride = ride_from_row(row)
    producer.send(topic_name, value=ride)
    if idx % log_every == 0:
        print(f"Sent {idx} messages...")
    # time.sleep(sleep_seconds)

producer.flush(timeout=10)

t1 = time.time()
print(f'took {(t1 - t0):.2f} seconds')