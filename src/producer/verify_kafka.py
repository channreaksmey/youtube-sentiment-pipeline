from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    "raw-youtube-comments",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    enable_auto_commit=False,
    value_deserializer=lambda v: json.loads(v.decode("utf-8"))
)

print("Reading messages from Kafka (showing first 5)...\n")

count = 0
for message in consumer:
    data = message.value
    print(f"Message {count + 1}:")
    print(f"  Author: {data['author']}")
    print(f"  Text: {data['text'][:80]}...")
    print(f"  Video: {data['video_id']}")
    print(f"  Likes: {data['like_count']}")
    print("-" * 60)
    
    count += 1
    if count >= 5:
        break

print(f"\nTotal messages read: {count}")
consumer.close()