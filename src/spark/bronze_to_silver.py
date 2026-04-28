import os
import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, regexp_replace, length, when, to_timestamp, current_timestamp, udf
from pyspark.sql.types import BooleanType

# PostgreSQL config
JDBC_URL = "jdbc:postgresql://localhost:5432/youtube_dw"
DB_PROPERTIES = {
    "user": "de_user",
    "password": "de_pass",
    "driver": "org.postgresql.Driver"
}

BRONZE_PATH = "data/bronze/comments"

def create_spark_session():
    return SparkSession.builder \
        .appName("YouTubeCommentsSilver") \
        .config("spark.jars", "https://jdbc.postgresql.org/download/postgresql-42.7.3.jar") \
        .getOrCreate()

# UDF to detect URLs
def contains_url(text):
    if text is None:
        return False
    url_pattern = r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+'
    return bool(re.search(url_pattern, str(text)))

contains_url_udf = udf(contains_url, BooleanType())

def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    print("Reading Bronze data...")
    
    # Read all Bronze Parquet files
    bronze_df = spark.read.parquet(BRONZE_PATH)
    
    print(f"Bronze records: {bronze_df.count()}")
    
    # Transform to Silver
    silver_df = bronze_df \
        .dropDuplicates(["comment_id"]) \
        .withColumn("text_cleaned", 
            regexp_replace(
                regexp_replace(lower(col("text")), r"http\S+|www.\S+", ""),
                r"[^a-zA-Z0-9\s]", ""
            )
        ) \
        .withColumn("text_cleaned", regexp_replace(col("text_cleaned"), r"\s+", " ")) \
        .withColumn("text_length", length(col("text_cleaned"))) \
        .withColumn("has_url", contains_url_udf(col("text"))) \
        .withColumn("published_at", to_timestamp(col("published_at"))) \
        .withColumn("language", lower(regexp_replace(col("author"), r".*", "en"))) \
        .withColumn("processed_at", current_timestamp()) \
        .select(
            "comment_id",
            "video_id",
            "author",
            "text",
            "text_cleaned",
            "published_at",
            "like_count",
            "text_length",
            "has_url",
            "language",
            "fetched_at",
            "processed_at"
        )
    
    print(f"Silver records after dedup: {silver_df.count()}")
    print("\nSample Silver data:")
    silver_df.show(5, truncate=False)
    
    # Write to PostgreSQL
    print("Writing to PostgreSQL...")
    silver_df.write \
        .jdbc(url=JDBC_URL, table="silver_comments", mode="overwrite", properties=DB_PROPERTIES)
    
    print("Silver layer written successfully!")
    
    # Show table stats
    silver_df.groupBy("video_id").count().orderBy(col("count").desc()).show(10)

if __name__ == "__main__":
    main()