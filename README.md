# YouTube Real-Time Sentiment Analytics Pipeline

An end-to-end data engineering pipeline that ingests YouTube comments in real-time, processes them through Bronze-Silver-Gold medallion architecture, performs sentiment analysis using a pre-trained Hugging Face model, and visualizes insights through an interactive Streamlit dashboard.

## Architecture

```
YouTube Data API v3
        │
        ▼
   Apache Kafka (Message Broker)
        │
        ▼
┌─────────────────┐
│  Bronze Layer   │  ← Raw JSON comments (Parquet)
│  (PySpark Batch)│
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Silver Layer   │  ← Cleaned, deduplicated (PostgreSQL)
│  (PySpark Batch)│     HTML decoded, URLs removed
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   Gold Layer    │  ← Star Schema (PostgreSQL)
│  (PySpark + HF) │     Sentiment scored via distilbert
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Pentaho PDI    │  ← Batch ETL Orchestration
│  (.kjb + .ktr)  │     Silver → daily_video_stats
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   Streamlit     │  ← Interactive Dashboard
│   Dashboard     │     Real-time KPIs & visualizations
└─────────────────┘
```

## Technology Stack

| Component | Technology |
|-----------|-----------|
| Data Source | YouTube Data API v3 |
| Message Broker | Apache Kafka |
| Streaming Ingestion | Python + kafka-python |
| Batch Processing | PySpark + Pentaho PDI |
| Data Warehouse | PostgreSQL |
| ML Inference | Hugging Face Transformers (distilbert) |
| Dashboard | Streamlit + Plotly |
| Infrastructure | Docker Compose |

## Prerequisites

- Python 3.10+
- Docker & Docker Compose
- YouTube Data API v3 key ([Get one here](https://console.cloud.google.com/))
- Hugging Face token (optional, for higher rate limits)

## Quick Start

### 1. Clone & Setup

```bash
git clone <your-repo-url>
cd youtube-sentiment-pipeline

# Use uv for lightning-fast setup
uv sync
```

### 2. Configure API Keys

Create `config/.env`:

```env
YOUTUBE_API_KEY=your_youtube_api_key_here
HF_TOKEN=your_huggingface_token_here  # optional
```

### 3. Start Infrastructure

```bash
docker-compose up -d
```

### 4. Run the Pipeline

#### Option A: Full Automation (Recommended)

```bash
uv run run_pipeline.py
```

Runs all steps automatically:
1. Producer (2 minutes of ingestion)
2. Bronze Layer
3. Silver Layer
4. Gold Layer (Emotion Detection)
5. Pentaho Aggregation

#### Option B: Manual Step-by-Step

```bash
# Terminal 1: Ingest Comments (2 minutes)
uv run src/producer/youtube_producer.py

# Terminal 2: Bronze Layer
uv run src/spark/bronze_batch.py

# Terminal 3: Silver Layer
uv run src/spark/bronze_to_silver.py

# Terminal 4: Gold Layer (Emotion Detection)
uv run src/spark/gold_emotion_pipeline.py

# Terminal 5: Pentaho Aggregation
uv run pentaho/run_pentaho_batch.py
```

#### Terminal 6: Dashboard

```bash
uv run streamlit run src/dashboard/app.py
```

Opens interactive dashboard at `http://localhost:8501`.

---

## Pipeline Details

### Producer (`src/producer/youtube_producer.py`)

- Searches YouTube with rotating queries for variety
- Fetches top-level comments with pagination
- Sends JSON messages to Kafka topic `raw-youtube-comments`
- Respects API rate limits

### Bronze Layer (`src/spark/bronze_batch.py`)

- Batch reads from Kafka (earliest to latest offsets)
- Parses JSON schema
- Appends processing timestamp
- Writes to Parquet format

### Silver Layer (`src/spark/bronze_to_silver.py`)

**Transformations:**
- HTML entity decoding (`&#39;` → `'`, `&quot;` → `"`)
- HTML tag removal (`<a>`, `<br>`, etc.)
- URL detection and flagging
- Text normalization (lowercase, whitespace cleanup)
- Deduplication by `comment_id`
- Language tagging

**Output Schema:**

| Column | Type | Description |
|--------|------|-------------|
| comment_id | VARCHAR(PK) | Unique YouTube comment ID |
| video_id | VARCHAR | YouTube video ID |
| author | VARCHAR | Comment author |
| text | TEXT | Raw comment text |
| text_cleaned | TEXT | Cleaned text |
| published_at | TIMESTAMP | Original post time |
| like_count | INTEGER | Number of likes |
| text_length | INTEGER | Cleaned text length |
| has_url | BOOLEAN | Contains URL |
| language | VARCHAR | Detected language |
| fetched_at | TIMESTAMP | Ingestion time |
| processed_at | TIMESTAMP | Processing time |

### Gold Layer (`src/spark/silver_to_gold_hf.py`)

**Star Schema:**

**Fact Table: `fact_sentiment`**
| Column | Type | Description |
|--------|------|-------------|
| sentiment_sk | BIGINT(PK) | Surrogate key |
| comment_id | VARCHAR | Reference to comment |
| time_sk | INTEGER | FK to dim_time |
| video_sk | INTEGER | FK to dim_video |
| author_sk | INTEGER | FK to dim_author |
| sentiment_label | VARCHAR | positive / negative / neutral |
| confidence | FLOAT | Model confidence (0-1) |
| like_count | INTEGER | Number of likes |

**Dimension Tables:**
- `dim_time` — hour, day_of_week, is_weekend, month, year
- `dim_video` — video_id, title, channel, category
- `dim_author` — author_name, total_comments, avg_likes, max_likes

**Sentiment Model:**
- Model: `distilbert-base-uncased-finetuned-sst-2-english`
- Labels: positive, negative
- Confidence threshold: model output score
- Neutral assigned to low-confidence predictions

### Pentaho Batch ETL (`pentaho/run_pentaho_batch.py`)

Replicates a Pentaho PDI transformation (`silver_to_gold.ktr`):

**Steps:**
1. **Table Input**: Reads `silver_comments` from PostgreSQL
2. **Group By**: Aggregates by `video_id` and `DATE(published_at)`
3. **Calculator**: Computes `total_comments`, `avg_likes`, `avg_length`, `url_count`
4. **Table Output**: Writes to `daily_video_stats`

**Job Orchestration (`run_pipeline.kjb`):**
```
START → Run Producer → Run Bronze → Run Silver → Run Gold → Run Pentaho Aggregation → Success
```

**Files:**
- `pentaho/jobs/run_pipeline.kjb` — Master orchestration job (XML)
- `pentaho/jobs/silver_to_gold.ktr` — Aggregation transformation (XML)
- `pentaho/run_pentaho_batch.py` — Python implementation of the transformation
- `run_pipeline.py` — Python orchestrator (equivalent to `kitchen.sh`)

### Dashboard (`src/dashboard/app.py`)

**Features:**
- Real-time KPI cards (total, positive%, neutral%, negative%)
- Sentiment trends over time (hourly)
- Confidence distribution histogram
- Sentiment breakdown by video
- Top authors by sentiment
- Model confidence analysis
- High-confidence prediction samples

---

## Data Flow Summary

| Stage | Records | Storage | Format |
|-------|---------|---------|--------|
| Kafka | ~1,700+ | Kafka topic | JSON |
| Bronze | ~1,700+ | `data/bronze/comments` | Parquet |
| Silver | ~1,680 (after dedup) | PostgreSQL | SQL |
| Gold | ~1,680 | PostgreSQL | Star Schema |
| Pentaho Aggregate | ~8-10 rows | PostgreSQL | daily_video_stats |

---

## Project Structure

```
youtube-sentiment-pipeline/
├── config/
│   └── .env                          # API keys (gitignored)
├── data/
│   ├── bronze/                       # Raw Parquet files (gitignored)
│   ├── silver/                       # Intermediate (gitignored)
│   └── checkpoints/                  # Spark checkpoints (gitignored)
├── pentaho/
│   ├── jobs/
│   │   ├── run_pipeline.kjb          # Pentaho job orchestration
│   │   └── silver_to_gold.ktr        # Pentaho transformation
│   ├── generate_ktr.py               # Generates .ktr XML file
│   ├── run_pentaho_batch.py          # Python simulation of Pentaho job
│   └── README.md                     # Pentaho documentation
├── src/
│   ├── producer/
│   │   ├── youtube_producer.py       # YouTube API → Kafka
│   │   ├── test_api.py               # API connectivity test
│   │   ├── verify_kafka.py           # Kafka message checker
│   │   └── kafka_check.py            # Quick Kafka diagnostic
│   ├── spark/
│   │   ├── bronze_batch.py           # Kafka → Bronze Parquet
│   │   ├── bronze_to_silver.py       # Bronze → Silver PostgreSQL
│   │   ├── silver_to_gold_hf.py      # Silver → Gold Star Schema + ML
│   │   └── silver_to_gold.py         # Silver → Gold (rule-based fallback)
│   └── dashboard/
│       └── app.py                    # Streamlit dashboard
├── docker-compose.yml                # All infrastructure
├── requirements.txt                  # Python dependencies
├── run_pipeline.py                   # Python orchestrator (equivalent to kitchen.sh)
└── README.md                         # This file
```

---

## Common Issues

### Kafka shows 0 messages

```bash
# Check if producer is running
python src/producer/kafka_check.py

# If empty, restart producer and let it run 3-5 minutes
python src/producer/youtube_producer.py
```

### Spark Kafka connector error

Ensure you're using the correct Scala version:
```python
# For Spark 4.x (Scala 2.13)
.config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1")

# For Spark 3.x (Scala 2.12)
.config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1")
```

### Hugging Face rate limit warning

Set a token in `config/.env`:
```env
HF_TOKEN=hf_your_token_here
```

### PostgreSQL connection refused

```bash
# Check container is running
docker ps

# Restart if needed
docker-compose restart postgres
```

---

## API Quota Management

YouTube Data API v3 free tier: **10,000 quota units/day**

| Operation | Cost | Daily Max |
|-----------|------|-----------|
| Search | 100 units | ~100 searches |
| Comment threads | 1 unit | ~10,000 calls |

**Typical usage:**
- 10 video searches: 1,000 units
- 50 comments per video: 10 videos × 1 call each (50 comments fits in 1 call)
- **Total per batch: ~1,010 units**
- **Max batches/day: ~9 batches**

---

## Pentaho PDI Notes

### Why Python Orchestrator?

The `run_pipeline.py` script is equivalent to running:
```bash
/opt/pentaho-pdi/kitchen.sh -file=jobs/run_pipeline.kjb
```

**Reasons for Python approach:**
- Pentaho PDI download availability issues (SourceForge redirects)
- ARM64 Mac compatibility (official images are AMD64)
- Faster iteration during development
- Same Kafka/PostgreSQL targets

**Production would use:**
```bash
# Build custom Pentaho image
docker build -t pentaho-pdi ./pentaho/

# Run job
docker run pentaho-pdi kitchen.sh -file=/pentaho-jobs/run_pipeline.kjb
```

---

## Postman

Check API

```bash
curl -X GET "http://localhost:8000/"
```

Run Pipline

```bash
curl -X POST "http://localhost:8000/pipeline/run"
```

Check Pipeline Status
```bash
curl -X GET "http://localhost:8000/pipeline/status"
```

View Logs

```bash
curl -X GET "http://localhost:8000/pipeline/logs"
```


---

## Future Enhancements

- [ ] Fine-tune distilbert on YouTube-specific comments
- [ ] Implement real-time streaming with Spark Structured Streaming
- [ ] Add Apache Airflow for pipeline orchestration
- [ ] Deploy to cloud (AWS/GCP)
- [ ] Add user authentication to dashboard

---

## Acknowledgments

- YouTube Data API v3 by Google
- Hugging Face Transformers library
- Apache Spark & Kafka communities
- Streamlit for dashboard framework
- Pentaho Data Integration by Hitachi Vantara
