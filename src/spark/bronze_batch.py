import os
from pyspark.sql.functions import col, from_json, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

from src.utils.spark import get_spark_session
from src.utils.config import KAFKA_BROKER, KAFKA_TOPIC, BRONZE_PATH

schema = StructType([
    StructField("comment_id", StringType(), True),
    StructField("video_id", StringType(), True),
    StructField("author", StringType(), True),
    StructField("text", StringType(), True),
    StructField("published_at", StringType(), True),
    StructField("like_count", IntegerType(), True),
    StructField("fetched_at", StringType(), True)
])

def main():
    spark = get_spark_session("YouTubeBronzeBatch")
    
    print("=" * 60)
    print("BRONZE BATCH JOB")
    print("=" * 60)
    
    # Read ALL messages from Kafka (batch mode)
    print(f"\nReading from Kafka ({KAFKA_BROKER})...")
    df = spark.read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "earliest") \
        .option("endingOffsets", "latest") \
        .load()
    
    kafka_count = df.count()
    print(f"Raw Kafka messages: {kafka_count}")
    
    if kafka_count == 0:
        print("\nNo data in Kafka!")
        spark.stop()
        return
    
    # Parse JSON
    print("\nParsing JSON...")
    parsed = df.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")
    
    # Filter out nulls
    parsed = parsed.filter(col("comment_id").isNotNull())
    parsed_count = parsed.count()
    
    # Add processing timestamp
    bronze_df = parsed.withColumn("processed_at", current_timestamp())
    
    # Write to Parquet
    print(f"\nWriting to Bronze: {BRONZE_PATH}")
    bronze_df.write \
        .format("parquet") \
        .mode("overwrite") \
        .save(BRONZE_PATH)
    
    print(f"Bronze layer complete: {parsed_count} records")
    spark.stop()

if __name__ == "__main__":
    main()
