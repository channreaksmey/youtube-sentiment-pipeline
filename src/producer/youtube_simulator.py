import os
import json
import time
import logging
import random
import uuid
from datetime import datetime, timedelta
from kafka import KafkaProducer

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Config
KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "raw-youtube-comments"

# Mock Data
AUTHORS = [
    "TechGuru", "GamingLife", "MovieBuff99", "ChefMario", "TravelWithMe",
    "ScienceExplorer", "HistoryBuff", "CodeMaster", "DataVizGuy", "MusicLover",
    "UrbanExplorer", "DIYQueen", "FitnessFanatic", "YogaWithSunny", "FinancePro",
    "LateNightTalker", "NewsJunkie", "SportsFanatic", "AnimeFan", "PetLovers"
]

MOCK_COMMENTS = [
    "Great video! Really enjoyed the content.",
    "This was so informative, thanks for sharing.",
    "I disagree with your point about the new update.",
    "Can you do a video on how to set up Spark in Docker?",
    "First! Love your channel.",
    "The editing in this video is amazing.",
    "I've been waiting for this review for a long time.",
    "Does anyone know the song at 3:45?",
    "This helped me so much with my project.",
    "What camera are you using to film this?",
    "Subscribed! Looking forward to more videos.",
    "Interesting perspective, hadn't thought of it that way.",
    "Worst video ever, you missed so many details.",
    "I love the soulsborne games, definitely my favorites.",
    "This tutorial was a bit too fast for me.",
    "Amazing work as always!",
    "Is this worth buying in 2026?",
    "The sentiment analysis part was very cool.",
    "Please do a follow-up video on the database setup.",
    "Greetings from Brazil! Love your content."
]

VIDEO_IDS = [f"vid_{i:03d}" for i in range(100)]

class YouTubeSimulator:
    def __init__(self):
        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER,
            value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
            acks='all',
            retries=3
        )
        logger.info(f"Simulator connected to Kafka at {KAFKA_BROKER}")

    def generate_comment(self):
        """Generate a single mock comment."""
        video_id = random.choice(VIDEO_IDS)
        author = random.choice(AUTHORS)
        text = random.choice(MOCK_COMMENTS)
        
        # Randomly add some "buzzwords" for sentiment analysis testing
        if random.random() < 0.3:
            text += " " + random.choice(["awesome", "terrible", "bad", "great", "sucks", "love it"])
            
        published_date = datetime.utcnow() - timedelta(days=random.randint(0, 30), minutes=random.randint(0, 1440))
        
        return {
            "comment_id": str(uuid.uuid4()),
            "video_id": video_id,
            "author": author,
            "text": text,
            "published_at": published_date.isoformat() + "Z",
            "like_count": random.randint(0, 100),
            "fetched_at": datetime.utcnow().isoformat() + "Z"
        }

    def stream_mock_data(self, comments_per_second=2):
        logger.info("Starting YouTube mock data stream...")
        try:
            while True:
                comment = self.generate_comment()
                self.producer.send(KAFKA_TOPIC, comment)
                
                if random.random() < 0.1:  # Occasionally log
                    logger.info(f"Generated mock comment for video {comment['video_id']}")
                
                time.sleep(1.0 / comments_per_second)
        except KeyboardInterrupt:
            logger.info("Simulator stopping...")
        finally:
            self.close()

    def close(self):
        """Cleanup resources."""
        self.producer.flush()
        self.producer.close()
        logger.info("Simulator closed")

if __name__ == "__main__":
    simulator = YouTubeSimulator()
    simulator.stream_mock_data(comments_per_second=5)
