"""
Pentaho PDI Batch Job Simulation
Aggregates Silver comments to daily video statistics.
Sentiment analysis is handled separately in the Gold layer.
"""

import psycopg2
from datetime import datetime

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "database": "youtube_dw",
    "user": "de_user",
    "password": "de_pass"
}

def run_pentaho_transformation():
    print("=" * 60)
    print("PENTAHO PDI BATCH JOB: silver_to_gold.ktr")
    print("=" * 60)
    print(f"Started at: {datetime.now()}")
    
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    # Step 1: Read Silver (Table Input)
    print("\n[Step 1/3] Reading Silver comments...")
    cursor.execute("SELECT COUNT(*) FROM silver_comments")
    silver_count = cursor.fetchone()[0]
    print(f"  Input records: {silver_count}")
    
    # Step 2: Transform & Aggregate (Group By + Calculator)
    print("\n[Step 2/3] Aggregating by video and date...")
    
    aggregation_sql = """
    INSERT INTO daily_video_stats (
        video_id, comment_date, total_comments, avg_likes,
        avg_length, url_count
    )
    SELECT 
        video_id,
        DATE(published_at) as comment_date,
        COUNT(*) as total_comments,
        ROUND(AVG(like_count)::NUMERIC, 2) as avg_likes,
        ROUND(AVG(text_length)::NUMERIC, 2) as avg_length,
        SUM(CASE WHEN has_url THEN 1 ELSE 0 END) as url_count
    FROM silver_comments
    GROUP BY video_id, DATE(published_at)
    ON CONFLICT (video_id, comment_date) DO UPDATE SET
        total_comments = EXCLUDED.total_comments,
        avg_likes = EXCLUDED.avg_likes,
        avg_length = EXCLUDED.avg_length,
        url_count = EXCLUDED.url_count,
        created_at = CURRENT_TIMESTAMP;
    """
    
    cursor.execute(aggregation_sql)
    conn.commit()
    
    # Step 3: Verify output (Table Output)
    print("\n[Step 3/3] Writing to daily_video_stats...")
    cursor.execute("SELECT COUNT(*) FROM daily_video_stats")
    gold_count = cursor.fetchone()[0]
    print(f"  Output records: {gold_count}")
    
    # Show sample
    print("\nSample output:")
    cursor.execute("""
        SELECT video_id, comment_date, total_comments, avg_likes, avg_length, url_count 
        FROM daily_video_stats 
        ORDER BY comment_date DESC 
        LIMIT 5
    """)
    for row in cursor.fetchall():
        print(f"  {row[0][:10]}... | {row[1]} | {row[2]} comments | avg likes: {row[3]}")
    
    cursor.close()
    conn.close()
    
    print(f"\nCompleted at: {datetime.now()}")
    print("=" * 60)
    print("PENTAHO BATCH JOB COMPLETE")
    print("=" * 60)

if __name__ == "__main__":
    run_pentaho_transformation()