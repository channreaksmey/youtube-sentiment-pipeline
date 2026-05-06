from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    "raw-youtube-comments",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    consumer_timeout_ms=5000
)

print("Checking Kafka messages...")
count = 0
for msg in consumer:
    count += 1
    if count <= 3:
        data = json.loads(msg.value)
        print(f"{count}. {data['author']}: {data['text'][:60]}...")
    
consumer.close()
print(f"\nTotal messages in Kafka: {count}")