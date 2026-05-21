import os
import re
import html
import pandas as pd
from pyspark.sql.functions import col, length, when, to_timestamp, current_timestamp, udf, lit
from pyspark.sql.types import BooleanType, StringType, StructType, StructField, FloatType

from src.utils.spark import get_spark_session
from src.utils.db import write_postgres
from src.utils.config import BRONZE_PATH
from src.nlp.emotion_analyzer import analyze_emotions_partition

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

# Schema for the NLP output
NLP_SCHEMA = StructType([
    StructField("comment_id", StringType(), True),
    StructField("emotion_label", StringType(), True),
    StructField("emotion_score", FloatType(), True)
])

def main():
    spark = get_spark_session("YouTubeCommentsSilver")
    
    print("Reading Bronze data...")
    if not os.path.exists(BRONZE_PATH) and not BRONZE_PATH.startswith("/opt"):
         print(f"Bronze path {BRONZE_PATH} not found locally.")
    
    bronze_df = spark.read.parquet(BRONZE_PATH)
    
    print(f"Bronze records: {bronze_df.count()}")
    
    # 1. Basic Cleaning and Transformation
    print("Cleaning text and formatting fields...")
    silver_base = bronze_df \
        .dropDuplicates(["comment_id"]) \
        .withColumn("text_cleaned", clean_text_udf(col("text"))) \
        .withColumn("text_length", length(col("text_cleaned"))) \
        .withColumn("has_url", contains_url_udf(col("text"))) \
        .withColumn("published_at", to_timestamp(col("published_at"))) \
        .withColumn("fetched_at", to_timestamp(col("fetched_at"))) \
        .withColumn("language", lit("en")) \
        .withColumn("processed_at", current_timestamp())
    
    # 2. Run NLP (Emotion Analysis)
    print("Running NLP Emotion Analysis...")
    nlp_input = silver_base.select("comment_id", "text_cleaned")
    nlp_results = nlp_input.mapInPandas(analyze_emotions_partition, schema=NLP_SCHEMA)
    
    # 3. Join NLP results and add Sentiment Label
    print("Finalizing Silver schema...")
    silver_df = silver_base.join(nlp_results, "comment_id", "left") \
        .withColumn(
            "sentiment_label",
            when(col("emotion_label").isin(["joy", "surprise"]), "positive")
            .when(col("emotion_label").isin(["anger", "disgust", "fear", "sadness"]), "negative")
            .otherwise("neutral")
        ) \
        .withColumn(
            "sentiment_score",
            when(col("sentiment_label") == "positive", col("emotion_score"))
            .when(col("sentiment_label") == "negative", -col("emotion_score"))
            .otherwise(lit(0.0))
        ) \
        .select(
            "comment_id", "video_id", "author", "text", "text_cleaned",
            "published_at", "like_count", "text_length", "has_url",
            "language", "sentiment_label", "sentiment_score", 
            "emotion_label", "emotion_score", "fetched_at", "processed_at"
        )
    
    print(f"Silver records after processing: {silver_df.count()}")
    
    # Write to PostgreSQL
    print("Writing to PostgreSQL (silver_comments)...")
    write_postgres(silver_df, "silver_comments")
    
    print("Silver layer with NLP augmentation complete!")
    spark.stop()

if __name__ == "__main__":
    main()
