import os
from pyspark.sql.functions import (
    col, hour, dayofweek, month, year, weekofyear,
    when, count, avg, max as spark_max,
    monotonically_increasing_id, lit
)
from src.utils.spark import get_spark_session
from src.utils.db import read_postgres, write_postgres

def create_dimensions(silver_df):
    """Builds Star Schema dimensions from Silver data."""
    print("Building dimensions...")
    
    # dim_time
    dim_time = silver_df.select("published_at").distinct() \
        .withColumn("time_value", col("published_at")) \
        .withColumn("hour", hour("published_at")) \
        .withColumn("day_of_week", dayofweek("published_at")) \
        .withColumn("is_weekend", when(dayofweek("published_at").isin([1, 7]), True).otherwise(False)) \
        .withColumn("month", month("published_at")) \
        .withColumn("year", year("published_at")) \
        .withColumn("week_of_year", weekofyear("published_at")) \
        .distinct() \
        .orderBy("time_value") \
        .withColumn("time_sk", monotonically_increasing_id())
    
    # dim_video
    dim_video = silver_df.select("video_id").distinct() \
        .orderBy("video_id") \
        .withColumn("video_sk", monotonically_increasing_id()) \
        .withColumn("title", lit("Unknown")) \
        .withColumn("channel", lit("Unknown")) \
        .withColumn("category", lit("General"))
        
    # dim_author
    dim_author = silver_df.groupBy("author").agg(
        count("*").alias("total_comments"),
        avg("like_count").alias("avg_likes"),
        spark_max("like_count").alias("max_likes")
    ).orderBy(col("total_comments").desc()) \
     .withColumn("author_sk", monotonically_increasing_id()) \
     .withColumnRenamed("author", "author_name")
     
    return dim_time, dim_video, dim_author

def main():
    spark = get_spark_session("YouTubeGoldEmotion")
    
    print("=" * 60)
    print("GOLD LAYER: STAR SCHEMA AGGREGATION")
    print("=" * 60)
    
    # 1. Read Silver data (now contains pre-calculated NLP scores)
    print("\nReading Silver data from PostgreSQL...")
    silver_df = read_postgres(spark, "silver_comments")
    
    total_records = silver_df.count()
    print(f"Silver records: {total_records}")
    
    if total_records == 0:
        print("No data in Silver layer. Run Bronze/Silver first.")
        return

    # 2. Build dimensions
    dim_time, dim_video, dim_author = create_dimensions(silver_df)
    
    # 3. Build fact table
    print("\nBuilding fact_sentiment using pre-calculated scores...")
    
    fact = silver_df.alias("scored") \
        .join(dim_time.alias("time"), col("scored.published_at") == col("time.time_value"), "left") \
        .join(dim_video.alias("video"), col("scored.video_id") == col("video.video_id"), "left") \
        .join(dim_author.alias("author"), col("scored.author") == col("author.author_name"), "left") \
        .select(
            monotonically_increasing_id().alias("sentiment_sk"),
            col("scored.comment_id"),
            col("time.time_sk"),
            col("video.video_sk"),
            col("author.author_sk"),
            col("scored.sentiment_label"),
            col("scored.sentiment_score"),
            col("scored.emotion_label"),
            col("scored.emotion_score").alias("confidence"),
            col("scored.like_count")
        )
    
    # 4. Write to Gold
    print("\nWriting Gold layer to PostgreSQL...")
    write_postgres(fact, "fact_sentiment")
    write_postgres(dim_time, "dim_time")
    write_postgres(dim_video, "dim_video")
    write_postgres(dim_author, "dim_author")
    
    print("\n" + "=" * 60)
    print("GOLD LAYER COMPLETE")
    print("=" * 60)
    
    spark.stop()

if __name__ == "__main__":
    main()
