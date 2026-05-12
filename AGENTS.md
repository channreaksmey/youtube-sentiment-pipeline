# AGENTS.md - YouTube Sentiment Pipeline

## Essential Commands

### Setup
```bash
# Use uv (not pip) for dependency management
uv sync

# Configure API keys in config/.env:
# YOUTUBE_API_KEY=your_key_here
# HF_TOKEN=your_hf_token_here (optional)

# Start all infrastructure
docker-compose up -d
```

### Pipeline Execution
```bash
# Run complete pipeline (recommended)
uv run run_pipeline.py

# Individual components (for debugging)
uv run src/producer/youtube_producer.py          # 2-minute ingestion
uv run src/spark/bronze_batch.py                 # Kafka → Parquet
uv run src/spark/bronze_to_silver.py             # Parquet → PostgreSQL
uv run src/spark/gold_emotion_pipeline.py        # + ML sentiment
uv run pentaho/run_pentaho_batch.py              # Daily aggregates

# Dashboard
streamlit run src/dashboard/app.py               # http://localhost:8501
```

### Testing & Diagnostics
```bash
# Test YouTube API connectivity
uv run src/producer/test_api.py

# Check Kafka messages
uv run src/producer/kafka_check.py

# Setup database tables
uv run setup_database.py
```

## Critical Architecture Notes

### Data Flow
```
YouTube API → Kafka → Bronze (Parquet) → Silver (PostgreSQL) → Gold (PostgreSQL + ML) → Pentaho → Dashboard
```

### Spark Jobs Run in Docker
- All PySpark scripts execute inside Docker containers
- Use `docker exec` pattern in orchestrator
- Kafka connector: `spark-sql-kafka-0-10_2.12:3.5.0`
- PostgreSQL connector: `postgresql:42.7.3`

### API Quota Limits
- YouTube Data API: 10,000 units/day
- Typical batch uses ~1,010 units
- Max ~9 batches per day

### Pentaho Alternative
- Python orchestrator (`run_pipeline.py`) replaces Pentaho PDI
- Avoids Docker image availability issues on ARM64
- Uses same PostgreSQL targets

## Configuration Requirements

### Environment Variables (config/.env)
```env
YOUTUBE_API_KEY=required
HF_TOKEN=optional (for higher HF rate limits)
```

### Database Connection
- Host: localhost:5432
- User: de_user / Pass: de_pass
- Database: youtube_dw

## Common Issues

### Kafka Shows 0 Messages
```bash
# Check producer status
uv run src/producer/kafka_check.py

# Restart and let run 3-5 minutes
uv run src/producer/youtube_producer.py
```

### Spark Kafka Connector Errors
- Use Scala 2.12 for Spark 3.x
- Package: `org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0`

### PostgreSQL Connection Refused
```bash
# Check container status
docker ps

# Restart if needed
docker-compose restart postgres
```

## Key Files

### Orchestrators
- `run_pipeline.py` - Main pipeline orchestrator
- `setup_database.py` - Database table creation

### Pipeline Components
- `src/producer/` - YouTube API → Kafka
- `src/spark/` - Bronze/Silver/Gold layers
- `pentaho/` - Daily aggregation (Python implementation)
- `src/dashboard/` - Streamlit visualization

### Configuration
- `config/.env` - API keys (gitignored)
- `docker-compose.yml` - Infrastructure setup
- `pyproject.toml` - Python dependencies (uv-based)