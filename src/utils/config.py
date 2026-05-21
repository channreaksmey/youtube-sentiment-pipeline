import os

try:
    from dotenv import load_dotenv
    # Load .env from the project root (assuming we run from root)
    load_dotenv("config/.env")
except ImportError:
    # If python-dotenv is not installed (e.g., inside basic Spark container),
    # we rely on environment variables being passed directly.
    pass

# Infrastructure
IS_DOCKER = os.path.exists("/.dockerenv") or os.environ.get("SPARK_HOME") is not None

# Kafka
KAFKA_HOST = "kafka" if IS_DOCKER else "localhost"
KAFKA_PORT = "29092" if IS_DOCKER else "9092"
KAFKA_BROKER = f"{KAFKA_HOST}:{KAFKA_PORT}"
KAFKA_TOPIC = "raw-youtube-comments"

# PostgreSQL
POSTGRES_HOST = "postgres" if IS_DOCKER else "localhost"
POSTGRES_DB = "youtube_dw"
POSTGRES_USER = "de_user"
POSTGRES_PASSWORD = "de_pass"
POSTGRES_PORT = "5432"

JDBC_URL = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
DB_PROPERTIES = {
    "user": POSTGRES_USER,
    "password": POSTGRES_PASSWORD,
    "driver": "org.postgresql.Driver"
}

# Paths
BASE_DIR = "/opt/spark/work-dir" if IS_DOCKER else "."
BRONZE_PATH = f"{BASE_DIR}/data/bronze/comments"

# NLP
HF_TOKEN = os.getenv("HF_TOKEN")
EMOTION_MODEL = "j-hartmann/emotion-english-distilroberta-base"
SENTIMENT_MODEL = "distilbert-base-uncased-finetuned-sst-2-english"
