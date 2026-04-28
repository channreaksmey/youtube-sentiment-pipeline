from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, hour, dayofweek, month, year, weekofyear,
    when, count, avg, sum as spark_sum, max as spark_max,
    monotonically_increasing_id, lit
)

# PostgreSQL config
JDBC_URL = "jdbc:postgresql://localhost:5432/youtube_dw"
DB_PROPERTIES = {
    "user": "de_user",
    "password": "de_pass",
    "driver": "org.postgresql.Driver"
}


def create_spark_session():
    return SparkSession.builder \
        .appName("YouTubeCommentsGold") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3") \
        .getOrCreate()


def create_dim_time(spark, silver_df):
    """Create time dimension from published_at timestamps."""
    print("Building dim_time...")
    
    time_df = silver_df.select("published_at").distinct()

    # Keep the original timestamp as `time_value` and add time attributes
    dim_time = time_df.select(
        col("published_at").alias("time_value"),
        hour(col("published_at")).alias("hour"),
        dayofweek(col("published_at")).alias("day_of_week"),
        when(dayofweek(col("published_at")).isin([1, 7]), True).otherwise(False).alias("is_weekend"),
        month(col("published_at")).alias("month"),
        year(col("published_at")).alias("year"),
        weekofyear(col("published_at")).alias("week_of_year")
    ).distinct()

    # Add surrogate integer key without a window to avoid single-partition execution.
    dim_time = dim_time.orderBy("time_value").withColumn("time_sk", monotonically_increasing_id())
    
    print(f"dim_time records: {dim_time.count()}")
    return dim_time


def create_dim_video(spark, silver_df):
    """Create video dimension with metadata."""
    print("Building dim_video...")
    
    # Since we only have video_id from API, enrich with what we have.
    dim_video = silver_df.select("video_id").distinct().orderBy("video_id").withColumn("video_sk", monotonically_increasing_id())
    
    # Add placeholder columns for enrichment.
    dim_video = dim_video.select(
        col("video_sk"),
        col("video_id"),
        lit("Unknown").alias("title"),      # Could be enriched via API
        lit("Unknown").alias("channel"),     # Could be enriched via API
        lit("General").alias("category")     # Could be enriched via API
    )
    
    print(f"dim_video records: {dim_video.count()}")
    return dim_video


def create_dim_author(spark, silver_df):
    """Create author dimension with aggregated stats."""
    print("Building dim_author...")
    
    dim_author = silver_df.groupBy("author").agg(
        count("*").alias("total_comments"),
        avg("like_count").alias("avg_likes"),
        spark_max("like_count").alias("max_likes")
    ).orderBy(col("total_comments").desc(), col("author")).withColumn("author_sk", monotonically_increasing_id())
    
    dim_author = dim_author.select(
        "author_sk",
        col("author").alias("author_name"),
        "total_comments",
        "avg_likes",
        "max_likes"
    )
    
    print(f"dim_author records: {dim_author.count()}")
    return dim_author


def create_fact_sentiment(spark, silver_df, dim_time, dim_video, dim_author):
    """Create fact table joining all dimensions."""
    print("Building fact_sentiment...")
    
    # For now, use simple sentiment rules (ML model comes later)
    # Positive: > 5 likes OR contains "good", "great", "amazing"
    # Negative: contains "bad", "terrible", "hate"
    # Neutral: everything else
    
    sentiment_df = silver_df.withColumn(
        "sentiment_label",
        when(
            (col("like_count") > 5) | 
            (col("text_cleaned").rlike("good|great|amazing|love|best|awesome")),
            "positive"
        ).when(
            col("text_cleaned").rlike("bad|terrible|hate|worst|awful|suck"),
            "negative"
        ).otherwise("neutral")
    ).withColumn(
        "sentiment_score",
        when(col("sentiment_label") == "positive", 1.0)
        .when(col("sentiment_label") == "negative", -1.0)
        .otherwise(0.0)
    ).withColumn(
        "confidence",
        when(col("like_count") > 10, 0.9)
        .when(col("like_count") > 5, 0.7)
        .otherwise(0.5)
    )
    
    # Join with dimensions to get surrogate keys
    # Join on the timestamp value (time_value) so types match, then use surrogate `time_sk`
    fact = sentiment_df \
        .join(dim_time, sentiment_df.published_at == dim_time.time_value, "left") \
        .join(dim_video, "video_id", "left") \
        .join(dim_author, sentiment_df.author == dim_author.author_name, "left") \
        .select(
            monotonically_increasing_id().alias("sentiment_sk"),
            col("comment_id"),
            col("time_sk"),
            col("video_sk"),
            col("author_sk"),
            col("sentiment_score"),
            col("sentiment_label"),
            col("confidence"),
            col("like_count")
        )
    
    print(f"fact_sentiment records: {fact.count()}")
    
    # Show distribution
    print("\nSentiment distribution:")
    fact.groupBy("sentiment_label").count().show()
    
    return fact


def write_to_postgres(df, table_name):
    """Write DataFrame to PostgreSQL."""
    print(f"Writing {table_name}...")
    df.write.jdbc(
        url=JDBC_URL,
        table=table_name,
        mode="overwrite",
        properties=DB_PROPERTIES
    )
    print(f"{table_name} written successfully!")


def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    print("=" * 60)
    print("SILVER TO GOLD TRANSFORMATION")
    print("=" * 60)
    
    # Read Silver data
    print("\nReading Silver data from PostgreSQL...")
    silver_df = spark.read \
        .jdbc(url=JDBC_URL, table="silver_comments", properties=DB_PROPERTIES)
    
    silver_count = silver_df.count()
    print(f"Silver records: {silver_count}")
    
    if silver_count == 0:
        print("No data in Silver layer. Run Bronze → Silver first.")
        return
    
    # Build dimensions
    dim_time = create_dim_time(spark, silver_df)
    dim_video = create_dim_video(spark, silver_df)
    dim_author = create_dim_author(spark, silver_df)
    
    # Build fact table
    fact_sentiment = create_fact_sentiment(spark, silver_df, dim_time, dim_video, dim_author)
    
    # Write all to Gold
    print("\nWriting Gold layer to PostgreSQL...")
    # Overwrite the fact table first to remove any foreign-key dependencies
    # that would block dropping/overwriting dimension tables.
    write_to_postgres(fact_sentiment, "fact_sentiment")
    write_to_postgres(dim_time, "dim_time")
    write_to_postgres(dim_video, "dim_video")
    write_to_postgres(dim_author, "dim_author")
    
    print("\n" + "=" * 60)
    print("GOLD LAYER COMPLETE")
    print("=" * 60)
    
    # Summary
    print("\nGold Layer Summary:")
    print(f"  dim_time:      {dim_time.count()} records")
    print(f"  dim_video:     {dim_video.count()} records")
    print(f"  dim_author:    {dim_author.count()} records")
    print(f"  fact_sentiment: {fact_sentiment.count()} records")


if __name__ == "__main__":
    main()