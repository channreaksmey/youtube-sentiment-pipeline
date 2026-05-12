import psycopg2
import time

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "database": "youtube_dw",
    "user": "de_user",
    "password": "de_pass"
}

TABLES = {
    "daily_video_stats": """
        CREATE TABLE IF NOT EXISTS daily_video_stats (
            video_id VARCHAR(255),
            comment_date DATE,
            total_comments INTEGER,
            avg_likes NUMERIC,
            avg_length NUMERIC,
            url_count INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (video_id, comment_date)
        )
    """,
    "silver_comments": """
        CREATE TABLE IF NOT EXISTS silver_comments (
            comment_id VARCHAR(255) PRIMARY KEY,
            video_id VARCHAR(255),
            author VARCHAR(255),
            text TEXT,
            text_cleaned TEXT,
            published_at TIMESTAMP,
            like_count INTEGER,
            text_length INTEGER,
            has_url BOOLEAN,
            language VARCHAR(10),
            fetched_at TIMESTAMP,
            processed_at TIMESTAMP
        )
    """,
    "dim_time": """
        CREATE TABLE IF NOT EXISTS dim_time (
            time_sk BIGINT PRIMARY KEY,
            time_value TIMESTAMP,
            hour INTEGER,
            day_of_week INTEGER,
            is_weekend BOOLEAN,
            month INTEGER,
            year INTEGER,
            week_of_year INTEGER
        )
    """,
    "dim_video": """
        CREATE TABLE IF NOT EXISTS dim_video (
            video_sk BIGINT PRIMARY KEY,
            video_id VARCHAR(255),
            title VARCHAR(255),
            channel VARCHAR(255),
            category VARCHAR(255)
        )
    """,
    "dim_author": """
        CREATE TABLE IF NOT EXISTS dim_author (
            author_sk BIGINT PRIMARY KEY,
            author_name VARCHAR(255),
            total_comments INTEGER,
            avg_likes NUMERIC,
            max_likes INTEGER
        )
    """,
    "fact_sentiment": """
        CREATE TABLE IF NOT EXISTS fact_sentiment (
            sentiment_sk BIGINT PRIMARY KEY,
            comment_id VARCHAR(255),
            time_sk BIGINT,
            video_sk BIGINT,
            author_sk BIGINT,
            sentiment_score NUMERIC,
            sentiment_label VARCHAR(20),
            confidence NUMERIC,
            like_count INTEGER
        )
    """,
    "pipeline_jobs": """
        CREATE TABLE IF NOT EXISTS pipeline_jobs (
            job_id VARCHAR(255) PRIMARY KEY,
            steps TEXT,
            status VARCHAR(50),
            start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            end_time TIMESTAMP,
            logs TEXT
        )
    """
}

def setup_db():
    print("Connecting to PostgreSQL...")
    retries = 5
    conn = None
    while retries > 0:
        try:
            conn = psycopg2.connect(**DB_CONFIG)
            break
        except Exception as e:
            print(f"Waiting for Postgres... ({retries})")
            time.sleep(2)
            retries -= 1
    
    if not conn:
        print("Could not connect to Postgres.")
        return

    cursor = conn.cursor()
    
    for table_name, sql in TABLES.items():
        print(f"Ensuring table exists: {table_name}")
        cursor.execute(sql)
    
    conn.commit()
    cursor.close()
    conn.close()
    print("Database setup complete!")

if __name__ == "__main__":
    setup_db()
