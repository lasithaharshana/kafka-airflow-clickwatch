# E-Commerce Clickstream Analytics Pipeline

A lambda architecture implementation for real-time and batch clickstream analytics, built with Apache Kafka, Spark Structured Streaming, Apache Airflow, and PostgreSQL.

[![Python 3.8+](https://img.shields.io/badge/Python-3.8%2B-blue.svg)](https://www.python.org/downloads/)
[![Apache Kafka](https://img.shields.io/badge/Kafka-7.5.0-black.svg)](https://kafka.apache.org/)
[![Apache Spark](https://img.shields.io/badge/Spark-3.5.0-orange.svg)](https://spark.apache.org/)
[![Apache Airflow](https://img.shields.io/badge/Airflow-3.1.6-017CEE.svg)](https://airflow.apache.org/)
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED.svg)](https://docs.docker.com/compose/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

---

## Table of Contents

- [Architecture Overview](#architecture-overview)
- [System Components](#system-components)
- [Technology Stack](#technology-stack)
- [Prerequisites](#prerequisites)
- [Getting Started](#getting-started)
- [Web Interfaces](#web-interfaces)
- [Project Structure](#project-structure)
- [Configuration](#configuration)
- [Testing](#testing)
- [Troubleshooting](#troubleshooting)
- [License](#license)
- [Author](#author)

---

## Architecture Overview

This project implements a **lambda architecture** consisting of a speed layer for real-time flash sale detection and a batch layer for daily user segmentation and analytics.

```
                        ┌──────────────────────────────────────────────────────┐
                        │                   SPEED LAYER                        │
                        │                                                      │
┌──────────────┐        │   ┌───────────┐     ┌────────────────┐               │
│   Event      │───────▶│   │   Kafka   │────▶│ Spark Streaming│──▶ Flash Sale │
│  Simulator   │        │   │  Broker   │     │  (10-min window)│    Alerts    │
│              │        │   └─────┬─────┘     └───────┬────────┘               │
└──────────────┘        │         │                   │                         │
                        │         │                   ▼                         │
                        │         │            Parquet Files                    │
                        └─────────┼────────────────────────────────────────────┘
                                  │
                        ┌─────────┼────────────────────────────────────────────┐
                        │         ▼           BATCH LAYER                      │
                        │   ┌───────────┐     ┌────────────────┐               │
                        │   │   JSONL   │────▶│    Airflow     │──▶ Daily      │
                        │   │   Logs    │     │  (Daily 2 AM)  │   Reports     │
                        │   └───────────┘     └───────┬────────┘               │
                        │                             │                        │
                        │                             ▼                        │
                        │                      ┌─────────────┐                 │
                        │                      │  PostgreSQL  │                │
                        │                      │  Analytics   │                │
                        │                      └─────────────┘                 │
                        └──────────────────────────────────────────────────────┘
```

## System Components

### 1. Event Simulator

Generates realistic e-commerce clickstream events and publishes them to Kafka in real time.

- **Event types**: `view` (70%), `add_to_cart` (20%), `purchase` (10%)
- **Product categories**: Smartphones, Laptops, Tablets, Headphones, Smartwatches, Cameras, Gaming, Accessories
- **Default parameters**: 1,000 users, 100 products, 0.1s interval
- **Dual output**: Kafka topic and daily JSONL log files (`clickstream_YYYY-MM-DD.jsonl`)

### 2. Speed Layer — Spark Structured Streaming

Consumes events from Kafka in real time and applies windowed aggregation to detect flash sale opportunities.

- **Window**: 10-minute sliding window with 5-minute slide interval
- **Flash sale alert condition**: `views > 100` AND `purchases < 5` (high interest, low conversion)
- **Output sinks**: Console alerts (30s trigger) and Parquet files (60s trigger)

### 3. Batch Layer — Apache Airflow

Orchestrates daily batch processing via a scheduled DAG.

- **DAG**: `clickstream_daily_batch` — runs daily at 02:00 UTC
- **User segmentation**:
  - **Buyers**: Users with at least one purchase event
  - **Window Shoppers**: Users with views or cart additions but no purchases
- **Analytics**: Top 5 viewed products, conversion rates by category
- **Storage**: JSON summary reports and PostgreSQL analytics database

### 4. Serving Layer — PostgreSQL

Stores structured analytics results for downstream consumption.

| Table | Description |
|-------|-------------|
| `daily_summary` | Aggregate metrics per day (events, buyer/shopper counts, buyer rate) |
| `user_segments` | Per-user segment classification (buyer or window_shopper) |
| `product_performance` | Top viewed products with rank |
| `category_conversion` | Category-level views, purchases, and conversion rates |
| `flash_sale_alerts` | Detected flash sale events from the streaming layer |

---

## Technology Stack

| Component | Technology | Version |
|-----------|-----------|---------|
| Message Broker | Apache Kafka (Confluent) | 7.5.0 |
| Cluster Coordination | Apache ZooKeeper | 7.5.0 |
| Stream Processing | Apache Spark Structured Streaming | 3.5.0 |
| Batch Orchestration | Apache Airflow | 3.1.6 |
| Analytics Database | PostgreSQL | 14 |
| Columnar Storage | Apache Parquet | — |
| Containerization | Docker Compose | v2 |
| Language | Python | 3.12 (Airflow), 3.9 (Simulator) |

---

## Prerequisites

- [Docker](https://docs.docker.com/get-docker/) and [Docker Compose](https://docs.docker.com/compose/install/) (v2+)
- Git
- Minimum 8 GB RAM recommended (multiple JVM-based services)

---

## Getting Started

### 1. Clone the Repository

```bash
git clone https://github.com/lasithaharshana/kafka-airflow-clickwatch.git
cd kafka-airflow-clickwatch
```

### 2. Start All Services

```bash
docker compose up -d
```

This launches the following services:

| Service | Container | Port |
|---------|-----------|------|
| ZooKeeper | `zookeeper` | 2181 |
| Kafka Broker | `kafka` | 9092 |
| Spark Master | `spark-master` | 8080, 7077 |
| Spark Worker | `spark-worker` | — |
| PostgreSQL | `postgres` | 5432 |
| Airflow Webserver | `airflow-webserver` | 8081 |
| Airflow Scheduler | `airflow-scheduler` | — |
| Airflow Triggerer | `airflow-triggerer` | — |
| Event Simulator | `simulator` | — |
| Streaming Consumer | `streaming` | — |

### 3. Verify Services

```bash
docker compose ps
```

### 4. View Real-Time Streaming Alerts

```bash
docker compose logs -f streaming
```

### 5. Stop All Services

```bash
docker compose down
```

To also remove persistent data volumes:

```bash
docker compose down -v
```

---

## Web Interfaces

### Apache Airflow

| | |
|------|-------|
| **URL** | http://localhost:8081 |
| **Username** | `admin` |
| **Password** | `admin` |

Airflow uses the **Simple Auth Manager**. Credentials are stored in `config/simple_auth_manager_passwords.json`. To change the password, edit that file and restart the webserver:

```bash
docker compose restart airflow-webserver
```

### Spark Master UI

| | |
|------|-------|
| **URL** | http://localhost:8080 |
| **Auth** | None required |

---

## Project Structure

```
kafka-airflow-clickwatch/
├── config/
│   ├── __init__.py
│   ├── config.py                          # Centralized configuration (env-var driven)
│   └── simple_auth_manager_passwords.json # Airflow login credentials
├── src/
│   ├── simulator/
│   │   ├── __init__.py
│   │   └── event_generator.py             # Clickstream event producer
│   ├── streaming/
│   │   ├── __init__.py
│   │   └── spark_consumer.py              # Spark Structured Streaming consumer
│   └── batch/
│       ├── __init__.py
│       ├── airflow_dag.py                 # Airflow DAG definition
│       ├── user_segmentation.py           # User segmentation & analytics
│       └── postgres_storage.py            # PostgreSQL analytics storage
├── tests/
│   ├── test_simulator/
│   │   └── test_event_generator.py
│   └── test_batch/
│       └── test_user_segmentation.py
├── data/
│   └── parquet/                           # Streaming parquet output
├── logs/
│   └── clickstream/                       # Daily JSONL event logs
├── docker-compose.yml                     # Service orchestration
├── Dockerfile.airflow                     # Airflow image (slim-3.1.6-python3.12)
├── Dockerfile.simulator                   # Simulator image (python:3.9-slim)
├── Dockerfile.streaming                   # Streaming image (spark:3.5.0-python3)
├── init-db.sql                            # PostgreSQL schema initialization
├── requirements.txt                       # Python dependencies
├── setup.py                               # Package configuration
├── pyproject.toml                         # Tool configuration (black, isort, pytest)
└── README.md
```

---

## Configuration

All configuration is managed through environment variables with sensible defaults defined in `config/config.py`.

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker address |
| `KAFKA_TOPIC` | `clickstream-events` | Kafka topic name |
| `SPARK_MASTER` | `local[*]` | Spark master URL |
| `WINDOW_DURATION` | `10 minutes` | Streaming aggregation window |
| `SLIDE_DURATION` | `5 minutes` | Window slide interval |
| `VIEWS_THRESHOLD` | `100` | Flash sale: minimum views |
| `PURCHASES_THRESHOLD` | `5` | Flash sale: maximum purchases |
| `SIMULATOR_INTERVAL` | `0.1` | Seconds between generated events |
| `NUM_USERS` | `1000` | Number of simulated users |
| `NUM_PRODUCTS` | `100` | Number of simulated products |

Docker Compose overrides these defaults for inter-container communication (e.g., `kafka:29092` for internal broker access).

---

## Testing

### Run All Tests

```bash
pip install -r requirements.txt
pytest
```

### Run with Coverage Report

```bash
pytest --cov=src --cov-report=html --cov-report=term
```

### Run Specific Test Modules

```bash
pytest tests/test_simulator/
pytest tests/test_batch/
```

---

## Troubleshooting

### Kafka Connection Issues

```bash
docker compose ps kafka
docker compose logs kafka
```

Ensure Kafka has fully started before the simulator and streaming services connect. The health check is configured with a 30-second start period.

### Spark Streaming Not Processing

```bash
docker compose logs streaming
```

Check the Spark Master UI at http://localhost:8080 to verify the worker is registered and the application is running.

### Airflow DAG Not Visible

```bash
docker compose logs airflow-scheduler
```

Ensure the DAG file is properly mounted to `/opt/airflow/dags` and is not paused in the Airflow UI.

### Airflow Login Issues

Credentials: **admin / admin**. If login fails, verify the password file is mounted correctly:

```bash
docker compose exec airflow-webserver cat /opt/airflow/config/simple_auth_manager_passwords.json
```

### PostgreSQL Connection

```bash
docker compose exec postgres psql -U airflow -d analytics -c "\dt"
```

---

## License

This project is licensed under the MIT License.

## Author

**Lasitha Harshana** — [GitHub](https://github.com/lasithaharshana)
