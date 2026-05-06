from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "raw-youtube-comments"
BRONZE_PATH = "data/bronze/comments"

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
    spark = SparkSession.builder \
        .appName("YouTubeBronzeBatch") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    print("=" * 60)
    print("BRONZE BATCH JOB")
    print("=" * 60)
    
    # Read ALL messages from Kafka (batch mode)
    print("\nReading from Kafka...")
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
        print("Make sure producer is running and sending messages.")
        spark.stop()
        return
    
    # Parse JSON
    print("\nParsing JSON...")
    parsed = df.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")
    
    # Filter out nulls (parsing failures)
    parsed = parsed.filter(col("comment_id").isNotNull())
    parsed_count = parsed.count()
    print(f"Parsed records: {parsed_count}")
    
    # Add processing timestamp
    bronze_df = parsed.withColumn("processed_at", current_timestamp())
    
    # Write to Parquet
    print(f"\nWriting to Bronze: {BRONZE_PATH}")
    bronze_df.write \
        .format("parquet") \
        .mode("overwrite") \
        .save(BRONZE_PATH)
    
    print(f"Bronze layer complete: {parsed_count} records")
    
    # Show sample
    print("\nSample Bronze data:")
    bronze_df.select("comment_id", "video_id", "author", "text").show(3, truncate=50)
    
    spark.stop()

if __name__ == "__main__":
    main()