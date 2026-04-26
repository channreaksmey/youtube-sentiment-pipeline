# YouTube Sentiment Pipeline

## Quick Start
1. `docker-compose up -d` — Start Kafka + PostgreSQL
2. `pip install -r requirements.txt` — Install Python deps
3. Add your YouTube API key to `producer.py`
4. `python test_youtube_api.py` — Verify API works
5. `python producer.py` — Stream comments to Kafka
6. `python consumer_test.py` — Verify Kafka messages

## Architecture
Partner A (You): API → Kafka → Bronze → Silver
Partner B (Them): Silver → Gold → ML → Dashboard

## Schema Contract
See `SCHEMA_CONTRACT.md` for Silver layer handoff details.