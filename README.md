# E-commerce Clickstream Analytics Pipeline

Lambda architecture implementation for real-time clickstream analytics with Apache Kafka, Spark Streaming, and Airflow.

## Quick Start

```bash
# Start all services
docker-compose up -d

# View streaming alerts
docker-compose logs -f streaming

# Access Airflow UI
open http://localhost:8081
```

## Architecture

- **Speed Layer**: Spark Structured Streaming for flash sale detection
- **Batch Layer**: Airflow DAG for daily user segmentation  
- **Serving Layer**: Console alerts, JSON reports, PostgreSQL analytics

## Services

- Kafka: Event ingestion (port 9092)
- Spark: Real-time processing (port 8080)
- Airflow: Batch orchestration (port 8081)
- PostgreSQL: Analytics storage (port 5432)

## Tech Stack

✅ Apache Kafka (Producer, Topics, Partitions)  
✅ Spark Structured Streaming  
✅ Apache Airflow (DAG orchestration)  
✅ PostgreSQL (Analytics database)  
✅ Parquet (Columnar storage)
