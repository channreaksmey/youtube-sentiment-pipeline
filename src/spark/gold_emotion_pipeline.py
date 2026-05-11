import os
from pyspark.sql.functions import (
    col, hour, dayofweek, month, year, weekofyear,
    when, count, avg, max as spark_max,
    monotonically_increasing_id, lit
)
from pyspark.sql.types import StructType, StructField, StringType, FloatType

from src.utils.spark import get_spark_session
from src.utils.db import read_postgres, write_postgres
from src.nlp.emotion_analyzer import analyze_emotions_partition

# Schema for the NLP output
EMOTION_SCHEMA = StructType([
    StructField("comment_id", StringType(), True),
    StructField("emotion_label", StringType(), True),
    StructField("emotion_score", FloatType(), True)
])

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
    print("GOLD LAYER: EMOTION & SENTIMENT PIPELINE")
    print("=" * 60)
    
    # 1. Read Silver data
    print("\nReading Silver data from PostgreSQL...")
    silver_df = read_postgres(spark, "silver_comments")
    
    total_records = silver_df.count()
    print(f"Silver records: {total_records}")
    
    if total_records == 0:
        print("No data in Silver layer. Run Bronze/Silver first.")
        return

    # 2. Run Optimized NLP (Emotion Analysis)
    print(f"\nRunning Advanced Emotion Analysis via mapInPandas...")
    # Select only required columns for the NLP task to minimize data transfer
    nlp_input = silver_df.select("comment_id", "text_cleaned")
    
    # This executes on workers, loading model once per partition
    emotion_results = nlp_input.mapInPandas(analyze_emotions_partition, schema=EMOTION_SCHEMA)
    
    # 3. Join NLP results back to main dataframe
    scored_df = silver_df.join(emotion_results, "comment_id", "left")
    
    # 4. Add derived sentiment (simple logic based on emotion if needed, 
    # or keep it separate. For now, we use emotion as the primary metric)
    scored_df = scored_df.withColumn(
        "sentiment_label",
        when(col("emotion_label").isin(["joy", "surprise"]), "positive")
        .when(col("emotion_label").isin(["anger", "disgust", "fear", "sadness"]), "negative")
        .otherwise("neutral")
    )

    # 5. Build dimensions
    dim_time, dim_video, dim_author = create_dimensions(scored_df)
    
    # 6. Build fact table
    print("\nBuilding fact_sentiment...")
    
    # Alias DataFrames to avoid ambiguity during joins
    df_scored = scored_df.alias("scored")
    df_time = dim_time.alias("time")
    df_video = dim_video.alias("video")
    df_author = dim_author.alias("author")
    
    fact = df_scored \
        .join(df_time, col("scored.published_at") == col("time.time_value"), "left") \
        .join(df_video, col("scored.video_id") == col("video.video_id"), "left") \
        .join(df_author, col("scored.author") == col("author.author_name"), "left") \
        .select(
            monotonically_increasing_id().alias("sentiment_sk"),
            col("scored.comment_id"),
            col("time.time_sk"),
            col("video.video_sk"),
            col("author.author_sk"),
            col("scored.sentiment_label"),
            col("scored.emotion_label"),
            col("scored.emotion_score").alias("confidence"),
            col("scored.like_count")
        )
    
    # 7. Write to Gold
    print("\nWriting Gold layer to PostgreSQL...")
    # Order matters if foreign keys are enforced (though here they are not yet)
    write_postgres(fact, "fact_sentiment")
    write_postgres(dim_time, "dim_time")
    write_postgres(dim_video, "dim_video")
    write_postgres(dim_author, "dim_author")
    
    print("\n" + "=" * 60)
    print("GOLD LAYER COMPLETE")
    print("=" * 60)
    
    # Quick statistics
    print("\nEmotion Distribution:")
    fact.groupBy("emotion_label").count().orderBy(col("count").desc()).show()
    
    spark.stop()

if __name__ == "__main__":
    main()
