from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, hour, dayofweek, month, year, weekofyear,
    when, count, avg, sum as spark_sum, max as spark_max,
    monotonically_increasing_id, row_number, lit, pandas_udf
)
from pyspark.sql.window import Window
import pandas as pd
from transformers import pipeline
import os
from dotenv import load_dotenv

HF_TOKEN = os.getenv("HF_TOKEN")

# Set it for Hugging Face
if HF_TOKEN:
    os.environ["HF_TOKEN"] = HF_TOKEN
    print("HF_TOKEN loaded from config")
else:
    print("Warning: No HF_TOKEN found. Using unauthenticated requests.")
HF_TOKEN = os.getenv("HF_TOKEN")

# Set it for Hugging Face
if HF_TOKEN:
    os.environ["HF_TOKEN"] = HF_TOKEN
    print("HF_TOKEN loaded from config")
else:
    print("Warning: No HF_TOKEN found. Using unauthenticated requests.")

# PostgreSQL config
JDBC_URL = "jdbc:postgresql://localhost:5432/youtube_dw"
DB_PROPERTIES = {
    "user": "de_user",
    "password": "de_pass",
    "driver": "org.postgresql.Driver"
}


def load_sentiment_model():
    """Load pre-trained Hugging Face sentiment model."""
    print("Loading distilbert sentiment model...")
    model = pipeline(
        "sentiment-analysis",
        model="distilbert-base-uncased-finetuned-sst-2-english",
        device=-1  # CPU
    )
    print("Model loaded successfully!")
    return model


# Global model instance (load lazily after SparkSession created to avoid
# SparkContext-related errors during module import)
SENTIMENT_MODEL = None


def analyze_sentiment(texts: pd.Series) -> pd.DataFrame:
    """
    Analyze sentiment using pre-trained Hugging Face model.
    Returns label ('positive'/'negative') and confidence score.
    """
    # Handle empty or null texts
    texts = texts.fillna("").astype(str)
    
    # Filter out empty strings
    valid_mask = texts.str.len() > 0
    results = []
    
    if valid_mask.any():
        valid_texts = texts[valid_mask].tolist()
        batch_results = SENTIMENT_MODEL(
            valid_texts,
            truncation=True,
            max_length=512,
            batch_size=32
        )
        
        # Map results back to all texts
        result_idx = 0
        for is_valid in valid_mask:
            if is_valid:
                r = batch_results[result_idx]
                results.append({
                    "label": r["label"].lower(),
                    "score": float(r["score"])
                })
                result_idx += 1
            else:
                results.append({
                    "label": "neutral",
                    "score": 0.0
                })
    else:
        results = [{"label": "neutral", "score": 0.0} for _ in texts]
    
    return pd.DataFrame(results)


def create_spark_session():
    return SparkSession.builder \
        .appName("YouTubeCommentsGoldHF") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3") \
        .getOrCreate()


def build_dimensions(spark, silver_df):
    """Build dimension tables from Silver data."""
    print("\nBuilding dimension tables...")
    
    # dim_time
    time_df = silver_df.select("published_at").distinct()
    dim_time = time_df.select(
        col("published_at").alias("time_original"),
        hour("published_at").alias("hour"),
        dayofweek("published_at").alias("day_of_week"),
        when(dayofweek("published_at").isin([1, 7]), True).otherwise(False).alias("is_weekend"),
        month("published_at").alias("month"),
        year("published_at").alias("year"),
        weekofyear("published_at").alias("week_of_year")
    ).distinct()
    
    window = Window.orderBy("time_original")
    dim_time = dim_time.withColumn("time_sk", row_number().over(window))
    
    # dim_video
    dim_video = silver_df.select("video_id").distinct().withColumn(
        "video_sk", row_number().over(Window.orderBy("video_id"))
    ).withColumn("title", lit("TBD")).withColumn("channel", lit("TBD")).withColumn("category", lit("General"))
    
    # dim_author
    dim_author = silver_df.groupBy("author").agg(
        count("*").alias("total_comments"),
        avg("like_count").alias("avg_likes"),
        spark_max("like_count").alias("max_likes")
    ).withColumn("author_sk", row_number().over(Window.orderBy(col("total_comments").desc())))
    
    print(f"  dim_time: {dim_time.count()} records")
    print(f"  dim_video: {dim_video.count()} records")
    print(f"  dim_author: {dim_author.count()} records")
    
    return dim_time, dim_video, dim_author


def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    # Load the HF model after SparkSession exists to avoid PySpark import-time
    # errors when creating pandas_udf return types.
    global SENTIMENT_MODEL
    if SENTIMENT_MODEL is None:
        SENTIMENT_MODEL = load_sentiment_model()

    # Register the pandas UDF now that SparkSession is available
    analyze_sentiment_udf = pandas_udf(analyze_sentiment, "struct<label:string,score:float>")
    
    print("=" * 70)
    print("SILVER TO GOLD WITH HUGGING FACE SENTIMENT")
    print("=" * 70)
    
    # Read Silver data
    print("\nReading Silver data from PostgreSQL...")
    silver_df = spark.read.jdbc(url=JDBC_URL, table="silver_comments", properties=DB_PROPERTIES)
    
    count = silver_df.count()
    print(f"Silver records: {count}")
    
    if count == 0:
        print("No data in Silver layer. Run Bronze → Silver first.")
        return
    
    # Apply sentiment analysis
    print("\nRunning Hugging Face sentiment analysis...")
    print("This may take a few minutes for large datasets...")
    
    scored_df = silver_df.withColumn(
        "sentiment_result", 
        analyze_sentiment_udf(col("text_cleaned"))
    ).select(
        "*",
        col("sentiment_result.label").alias("sentiment_label"),
        col("sentiment_result.score").alias("confidence")
    ).drop("sentiment_result")
    
    # Show sample results
    print("\nSample sentiment predictions:")
    scored_df.select(
        "text_cleaned", 
        "sentiment_label", 
        "confidence", 
        "like_count"
    ).show(10, truncate=50)
    
    # Sentiment distribution
    print("\nSentiment distribution:")
    scored_df.groupBy("sentiment_label").count().show()
    
    # Average confidence by label
    print("\nAverage confidence by label:")
    scored_df.groupBy("sentiment_label").avg("confidence").show()
    
    # Build dimensions
    dim_time, dim_video, dim_author = build_dimensions(spark, scored_df)
    
    # Build fact table
    print("\nBuilding fact_sentiment...")
    fact = scored_df \
        .join(dim_time, scored_df.published_at == dim_time.time_original, "left") \
        .join(dim_video, "video_id", "left") \
        .join(dim_author, scored_df.author == dim_author.author, "left") \
        .select(
            monotonically_increasing_id().alias("sentiment_sk"),
            "comment_id",
            col("time_sk"),
            col("video_sk"),
            col("author_sk"),
            "sentiment_label",
            "confidence",
            "like_count"
        )
    
    # Write to Gold
    print("\nWriting Gold tables to PostgreSQL...")
    
    dim_time.drop("time_original").write \
        .jdbc(url=JDBC_URL, table="dim_time", mode="overwrite", properties=DB_PROPERTIES)
    
    dim_video.write \
        .jdbc(url=JDBC_URL, table="dim_video", mode="overwrite", properties=DB_PROPERTIES)
    
    dim_author.write \
        .jdbc(url=JDBC_URL, table="dim_author", mode="overwrite", properties=DB_PROPERTIES)
    
    fact.write \
        .jdbc(url=JDBC_URL, table="fact_sentiment", mode="overwrite", properties=DB_PROPERTIES)
    
    print("\n" + "=" * 70)
    print("GOLD LAYER WITH HF SENTIMENT COMPLETE")
    print("=" * 70)
    
    print(f"\nFact table: {fact.count()} records")
    print(f"Sentiment breakdown:")
    fact.groupBy("sentiment_label").count().orderBy(col("count").desc()).show()
    
    print("\nTop confident positive predictions:")
    fact.filter(col("sentiment_label") == "positive") \
        .orderBy(col("confidence").desc()) \
        .select("sentiment_label", "confidence", "like_count") \
        .show(5)
    
    print("\nTop confident negative predictions:")
    fact.filter(col("sentiment_label") == "negative") \
        .orderBy(col("confidence").desc()) \
        .select("sentiment_label", "confidence", "like_count") \
        .show(5)


if __name__ == "__main__":
    main()