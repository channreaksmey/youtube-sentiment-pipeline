import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Kafka configuration
KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "raw-youtube-comments"
BRONZE_PATH = "data/bronze/comments"

# Define schema for incoming JSON
comment_schema = StructType([
    StructField("comment_id", StringType(), True),
    StructField("video_id", StringType(), True),
    StructField("author", StringType(), True),
    StructField("text", StringType(), True),
    StructField("published_at", StringType(), True),
    StructField("like_count", IntegerType(), True),
    StructField("fetched_at", StringType(), True)
])

def create_spark_session():
    return SparkSession.builder \
        .appName("YouTubeCommentsBronze") \
        .config("spark.sql.streaming.checkpointLocation", "data/checkpoints/bronze") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
        .getOrCreate()

def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    print("Starting Bronze layer streaming...")
    
    # Read from Kafka
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "latest") \
        .load()
    
    # Parse JSON value
    parsed_df = kafka_df.select(
        from_json(col("value").cast("string"), comment_schema).alias("data"),
        col("timestamp").alias("kafka_timestamp")
    ).select("data.*", "kafka_timestamp")
    
    # Add processing timestamp
    bronze_df = parsed_df.withColumn("processed_at", current_timestamp())
    
    # Write to Bronze (Parquet with append mode)
    query = bronze_df.writeStream \
        .format("parquet") \
        .option("path", BRONZE_PATH) \
        .outputMode("append") \
        .trigger(processingTime="10 seconds") \
        .start()
    
    print(f"Bronze streaming started. Writing to: {BRONZE_PATH}")
    print("Press Ctrl+C to stop...")
    
    query.awaitTermination()

if __name__ == "__main__":
    main()