import os
import re
import html
from pyspark.sql.functions import col, length, when, to_timestamp, current_timestamp, udf, lit
from pyspark.sql.types import BooleanType, StringType

from src.utils.spark import get_spark_session
from src.utils.db import write_postgres
from src.utils.config import BRONZE_PATH

# UDF to detect URLs
def contains_url(text):
    if text is None:
        return False
    url_pattern = r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+'
    return bool(re.search(url_pattern, str(text)))

contains_url_udf = udf(contains_url, BooleanType())

def clean_text(text):
    """
    Properly clean YouTube comment text:
    1. Decode HTML entities
    2. Remove HTML tags
    3. Remove URLs
    4. Remove extra whitespace
    """
    if text is None:
        return ""
    text = html.unescape(text)
    text = re.sub(r'<[^>]+>', ' ', text)
    text = re.sub(r'http\S+|www.\S+|https\S+', '', text, flags=re.IGNORECASE)
    text = re.sub(r'@\w+|#\w+', '', text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text.lower()

clean_text_udf = udf(clean_text, StringType())

def main():
    spark = get_spark_session("YouTubeCommentsSilver")
    
    print("Reading Bronze data...")
    if not os.path.exists(BRONZE_PATH) and not BRONZE_PATH.startswith("/opt"):
         print(f"Bronze path {BRONZE_PATH} not found locally.")
    
    bronze_df = spark.read.parquet(BRONZE_PATH)
    
    print(f"Bronze records: {bronze_df.count()}")
    
    # Transform to Silver
    silver_df = bronze_df \
        .dropDuplicates(["comment_id"]) \
        .withColumn("text_cleaned", clean_text_udf(col("text"))) \
        .withColumn("text_length", length(col("text_cleaned"))) \
        .withColumn("has_url", contains_url_udf(col("text"))) \
        .withColumn("published_at", to_timestamp(col("published_at"))) \
        .withColumn("fetched_at", to_timestamp(col("fetched_at"))) \
        .withColumn("language", lit("en")) \
        .withColumn("processed_at", current_timestamp()) \
        .select(
            "comment_id", "video_id", "author", "text", "text_cleaned",
            "published_at", "like_count", "text_length", "has_url",
            "language", "fetched_at", "processed_at"
        )
    
    print(f"Silver records after dedup: {silver_df.count()}")
    
    # Write to PostgreSQL
    print("Writing to PostgreSQL (silver_comments)...")
    write_postgres(silver_df, "silver_comments")
    
    print("Silver layer written successfully!")
    spark.stop()

if __name__ == "__main__":
    main()
