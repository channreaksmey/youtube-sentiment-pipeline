# YouTube Sentiment Pipeline

## Quick Start
1. `docker-compose up -d` — Start Kafka + PostgreSQL
2. `pip install -r requirements.txt` — Install Python deps
3.  Add your YouTube API key to `.env`
4. `python test_youtube_api.py` — Verify API works
5. `python producer.py` — Stream comments to Kafka
6. `python consumer_test.py` — Verify Kafka messages

## Architecture
```plain
┌─────────────────────────────────────┐
│      YouTube Data API v3            │
│  (Search → Video IDs → Comments)    │
└─────────────┬───────────────────────┘
              │ Python script polls API
              ▼
┌─────────────────────────────────────┐
│            Kafka                    │
│  Topic: raw-youtube-comments        │
│  {video_id, comment, author,        │
│   published_at, like_count, ...}    │
└─────────────┬───────────────────────┘
              │
       ┌──────┴──────┐
       ▼             ▼
┌────────────┐  ┌─────────────┐
│  PySpark   │  │   Pentaho   │
│ Streaming  │  │   Batch     │
│ (Bronze)   │  │ (Silver/Gold)│
└─────┬──────┘  └──────┬──────┘
      │                │
      ▼                ▼
┌─────────────────────────────────────┐
│         Data Warehouse              │
│  ┌────────┐ ┌────────┐ ┌────────┐ │
│  │ Bronze │ │ Silver │ │  Gold  │ │
│  │ (Raw   │ │ (Clean │ │ (Star  │ │
│  │  JSON) │ │  + NLP)│ │ Schema)│ │
│  └────────┘ └────────┘ └────────┘ │
└─────────────────────────────────────┘
              │
       ┌──────┴──────┐
       ▼             ▼
┌────────────┐  ┌─────────────┐
│  ML Model  │  │  Streamlit  │
│  (Sentiment│  │  Dashboard  │
│   + Topic) │  │             │
└────────────┘  └─────────────┘
```