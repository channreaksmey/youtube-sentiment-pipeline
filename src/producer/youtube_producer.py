import os
import json
import time
import logging
import random
from datetime import datetime
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from kafka import KafkaProducer
from dotenv import load_dotenv

SEARCH_QUERIES = [
    "technology review",
    "soulsborne games",
    "smartphone unboxing",
    "true crime",
    "programming tutorial",
    "movie review",
    "music video",
    "cooking tutorial",
    "unsoved mysteries",
    "travel vlog",
    "elden ring lore",  
    "science explained",
    "vlogbrothers",
    "book review",
    "boiler room"
]

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load config
load_dotenv("config/.env")
API_KEY = os.getenv("YOUTUBE_API_KEY")
KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "raw-youtube-comments"

class YouTubeKafkaProducer:
    def __init__(self):
        self.youtube = build("youtube", "v3", developerKey=API_KEY)
        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER,
            value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
            acks='all',
            retries=3
        )
        logger.info(f"Connected to Kafka at {KAFKA_BROKER}")

    def search_videos(self, query=None, max_results=5):

        # Pick random query if none provided
        if query is None:
            query = random.choice(SEARCH_QUERIES)
    
        # Add randomization to bust cache
        random_suffix = random.choice(["", " latest", " 2026", " new", " trending"])
        search_query = f"{query}{random_suffix}"
    
        logger.info(f"Searching for: '{search_query}'")
    
        try:
            response = self.youtube.search().list(
                q=search_query,
                part="id,snippet",
                type="video",
                order="relevance",  # You changed this
                maxResults=max_results,
                videoEmbeddable="true",
                # Add publishedAfter to get recent videos only
                publishedAfter="2026-01-01T00:00:00Z"
            ).execute()
        
            videos = []
            for item in response.get("items", []):
                video = {
                    "video_id": item["id"]["videoId"],
                    "title": item["snippet"]["title"],
                    "channel": item["snippet"]["channelTitle"],
                    "published_at": item["snippet"]["publishedAt"]
                }
                videos.append(video)
                logger.info(f"Found: {video['title'][:60]}...")
        
            return videos
        
        except HttpError as e:
            logger.error(f"YouTube API error: {e}")
            return []

    def fetch_comments(self, video_id, max_results=100):
        """Fetch top-level comments for a video."""
        comments = []
        try:
            request = self.youtube.commentThreads().list(
                part="snippet",
                videoId=video_id,
                maxResults=min(max_results, 100),  # API max is 100
                order="relevance"
            )
            
            while request and len(comments) < max_results:
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
                        "fetched_at": datetime.utcnow().isoformat() + "Z"
                    }
                    comments.append(comment)
                
                # Get next page if available
                request = self.youtube.commentThreads().list_next(request, response)
                
                if request:
                    time.sleep(0.5)  # Rate limiting
                
        except HttpError as e:
            logger.error(f"Error fetching comments for {video_id}: {e}")
        
        logger.info(f"Fetched {len(comments)} comments for video {video_id}")
        return comments

    def send_to_kafka(self, comment):
        """Send a single comment to Kafka."""
        try:
            future = self.producer.send(KAFKA_TOPIC, comment)
            record_metadata = future.get(timeout=10)
            logger.debug(f"Sent comment {comment['comment_id']} to partition {record_metadata.partition}")
            return True
        except Exception as e:
            logger.error(f"Failed to send comment to Kafka: {e}")
            return False

    def stream_comments(self, query=None, videos_per_batch=5, comments_per_video=100):
        logger.info(f"Starting comment stream (query: {query or 'random'})")
    
        while True:
            try:
                # Use provided query or random
                current_query = query or random.choice(SEARCH_QUERIES)
            
                videos = self.search_videos(current_query, max_results=videos_per_batch)
            
                for video in videos:
                    comments = self.fetch_comments(video["video_id"], comments_per_video)
                
                    for comment in comments:
                        self.send_to_kafka(comment)
                        time.sleep(0.05)  # Faster sending
                
                    logger.info(f"Streamed {len(comments)} from: {video['title'][:50]}")
                    time.sleep(1)
            
                logger.info(f"Batch done. Total videos this batch: {len(videos)}")
                time.sleep(20)  # Shorter wait between batches
            
            except KeyboardInterrupt:
                logger.info("Shutting down...")
                break
            except Exception as e:
                logger.error(f"Error: {e}")
                time.sleep(10)

    def close(self):
        """Cleanup resources."""
        self.producer.flush()
        self.producer.close()
        logger.info("Producer closed")

if __name__ == "__main__":
    producer = YouTubeKafkaProducer()
    try:
        producer.stream_comments(
            query=None,              # Random queries
            videos_per_batch=10,      # 10 videos per batch
            comments_per_video=50   # 50 comments each
        )
    except KeyboardInterrupt:
        print("\nStopped by user")
    finally:
        producer.close()