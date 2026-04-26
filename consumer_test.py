from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    "raw-youtube-comments",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",  # Read from beginning
    enable_auto_commit=True,
    value_deserializer=lambda v: json.loads(v.decode("utf-8"))
)

print("Listening for messages...")
for message in consumer:
    print(f"Received: {message.value['author']}: {message.value['text'][:60]}...")
    break  # Just show one, then exit