import json
import time
import os
from kafka import KafkaProducer
from dotenv import load_dotenv
from googleapiclient.discovery import build

load_dotenv()
API_KEY = os.getenv("YOUTUBE_API")
KAFKA_TOPIC = "raw-youtube-comments"

youtube = build("youtube", "v3", developerKey=API_KEY)
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def fetch_and_stream(video_id, max_comments=10):
    request = youtube.commentThreads().list(
        part="snippet",
        videoId=video_id,
        maxResults=max_comments,
        order="time"
    )
    response = request.execute()
    
    for item in response.get("items", []):
        snippet = item["snippet"]["topLevelComment"]["snippet"]
        comment = {
            "comment_id": item["id"],
            "video_id": video_id,
            "author": snippet["authorDisplayName"],
            "text": snippet["textDisplay"],
            "published_at": snippet["publishedAt"],
            "like_count": snippet.get("likeCount", 0),
            "fetched_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        }
        producer.send(KAFKA_TOPIC, comment)
        print(f"Sent: {comment['author']} - {comment['text'][:50]}...")
        time.sleep(0.5)  # Be polite to API
    
    producer.flush()
    print(f"Streamed {len(response.get('items', []))} comments to Kafka")

if __name__ == "__main__":
    # Use the video ID from your test script
    VIDEO_ID = "Vo6QTBMdUfU"
    fetch_and_stream(VIDEO_ID, max_comments=20)