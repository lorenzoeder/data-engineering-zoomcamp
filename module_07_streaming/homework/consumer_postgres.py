from ride_utils import ride_deserializer
from kafka import KafkaConsumer
from datetime import datetime

server = 'localhost:9092'
topic_name = 'green-trips'

consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers=[server],
    auto_offset_reset='earliest',
    group_id='rides-to-postgres',
    value_deserializer=ride_deserializer
)

import psycopg2

conn = psycopg2.connect(
    host='localhost',
    port=5432,
    database='postgres',
    user='postgres',
    password='postgres'
)
conn.autocommit = True
cur = conn.cursor()

print(f"Listening to {topic_name} and writing to PostgreSQL...")

cur.execute(
    """CREATE TABLE IF NOT EXISTS processed_events (
           PULocationID INTEGER,
           DOLocationID INTEGER,
           trip_distance DOUBLE PRECISION,
           total_amount DOUBLE PRECISION,
           pickup_datetime TIMESTAMP
       )"""
)

count = 0
inserted = 0
failed = 0
for message in consumer:
    ride = message.value
    count += 1
    pickup_datetime = None
    if ride.lpep_pickup_datetime:
        try:
            pickup_datetime = datetime.fromisoformat(ride.lpep_pickup_datetime)
        except ValueError:
            failed += 1
            if failed % 10 == 0:
                print(f"Failed to parse {failed} records so far...")
            continue

    try:
        cur.execute(
            """INSERT INTO processed_events
               (PULocationID, DOLocationID, trip_distance, total_amount, pickup_datetime)
               VALUES (%s, %s, %s, %s, %s)""",
            (ride.PULocationID, ride.DOLocationID,
             ride.trip_distance, ride.total_amount, pickup_datetime)
        )
        inserted += 1
    except Exception as exc:
        failed += 1
        if failed <= 5:
            print(f"Insert failed: {exc}")

    if count % 100 == 0:
        print(f"Processed {count} messages | inserted={inserted} failed={failed}")

consumer.close()
cur.close()
conn.close()