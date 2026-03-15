from kafka import KafkaConsumer
from ride_utils import ride_deserializer

server = "localhost:9092"
topic_name = "green-trips"

consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers=[server],
    auto_offset_reset="earliest",
    group_id="rides-console",
    value_deserializer=ride_deserializer,
)

count_gt_5 = 0

print(f"Listening to {topic_name}... Press Ctrl+C to stop.")

try:
    for message in consumer:
        ride = message.value
        if float(getattr(ride, "trip_distance", 0.0) or 0.0) > 5:
            count_gt_5 += 1
except KeyboardInterrupt:
    pass
finally:
    consumer.close()
    print(f"Trips with trip_distance > 5: {count_gt_5}")