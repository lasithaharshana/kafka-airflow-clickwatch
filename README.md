# E-commerce Clickstream Analytics Platform

A comprehensive real-time and batch processing system for e-commerce clickstream analytics using Kafka, Spark, and Airflow.

## 🏗️ Architecture

This project implements a modern data engineering pipeline with the following components:

```
┌──────────────┐     ┌───────┐     ┌──────────────┐     ┌─────────────┐
│  Event       │────▶│ Kafka │────▶│    Spark     │────▶│   Alerts    │
│  Simulator   │     │       │     │  Streaming   │     │ (Flash Sale)│
└──────────────┘     └───┬───┘     └──────────────┘     └─────────────┘
                         │
                         │
                    ┌────▼────┐     ┌──────────────┐     ┌─────────────┐
                    │  Logs   │────▶│   Airflow    │────▶│   Email/    │
                    │  Store  │     │    Batch     │     │   Summary   │
                    └─────────┘     └──────────────┘     └─────────────┘
```

### Components

1. **Event Simulator**: Generates realistic e-commerce clickstream events
   - User interactions: view, add_to_cart, purchase
   - Publishes to Kafka topic

2. **Streaming Analytics** (Spark Structured Streaming):
   - 10-minute sliding window aggregation
   - Tracks views and purchases per product
   - Flash sale alerts when views > 100 and purchases < 5

3. **Batch Processing** (Airflow):
   - Daily user segmentation: Window Shoppers vs Buyers
   - Top 5 viewed products report
   - Email/text summary generation

## 🚀 Quick Start

### Prerequisites

- Docker and Docker Compose
- Python 3.8+
- Git

### Installation

1. **Clone the repository**
   ```bash
   git clone https://github.com/lasithaharshana/biodata.git
   cd biodata
   ```

2. **Set up environment variables**
   ```bash
   cp .env.example .env
   # Edit .env with your configuration
   ```

3. **Start all services with Docker Compose**
   ```bash
   docker-compose up -d
   ```

   This will start:
   - Zookeeper (port 2181)
   - Kafka (port 9092)
   - Spark Master (port 8080)
   - Spark Worker
   - PostgreSQL (port 5432)
   - Airflow Webserver (port 8081)
   - Airflow Scheduler
   - Event Simulator
   - Streaming Consumer

4. **Verify services are running**
   ```bash
   docker-compose ps
   ```

### Access Web UIs

- **Spark Master UI**: http://localhost:8080
- **Airflow UI**: http://localhost:8081
  - Username: `airflow`
  - Password: `airflow`

## 📊 Usage

### Running Locally (Development)

1. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

2. **Run the Event Simulator**
   ```bash
   python -m src.simulator.event_generator
   ```

3. **Run Spark Streaming Consumer**
   ```bash
   spark-submit \
     --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
     src/streaming/spark_consumer.py
   ```

4. **Run Batch Processing**
   ```bash
   python -m src.batch.user_segmentation
   ```

### Configuration

Edit the `.env` file or environment variables:

```bash
# Kafka
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPIC=clickstream-events

# Alert Thresholds
VIEWS_THRESHOLD=100
PURCHASES_THRESHOLD=5

# Simulator
SIMULATOR_INTERVAL=0.1
NUM_USERS=1000
NUM_PRODUCTS=100
```

## 🧪 Testing

### Run all tests
```bash
pytest
```

### Run with coverage
```bash
pytest --cov=src --cov-report=html
```

### Run specific test modules
```bash
pytest tests/test_simulator/
pytest tests/test_batch/
```

## 🎨 Code Quality

### Format code
```bash
black src/ tests/ config/
isort src/ tests/ config/
```

### Lint code
```bash
flake8 src/ tests/ config/
```

### Run pre-commit hooks
```bash
pre-commit install
pre-commit run --all-files
```

## 📁 Project Structure

```
.
├── config/                 # Configuration files
│   ├── __init__.py
│   └── config.py
├── src/                    # Source code
│   ├── simulator/          # Event generator
│   │   ├── __init__.py
│   │   └── event_generator.py
│   ├── streaming/          # Spark streaming consumer
│   │   ├── __init__.py
│   │   └── spark_consumer.py
│   └── batch/              # Airflow batch processing
│       ├── __init__.py
│       ├── airflow_dag.py
│       └── user_segmentation.py
├── tests/                  # Test files
│   ├── test_simulator/
│   └── test_batch/
├── logs/                   # Log files
├── data/                   # Data storage
├── docker-compose.yml      # Docker orchestration
├── Dockerfile.simulator    # Simulator container
├── Dockerfile.streaming    # Streaming container
├── requirements.txt        # Python dependencies
├── setup.py                # Package setup
├── pyproject.toml          # Tool configurations
├── .pre-commit-config.yaml # Pre-commit hooks
├── .flake8                 # Flake8 configuration
├── .gitignore              # Git ignore rules
└── README.md               # This file
```

## 🔧 Development

### Setting up Development Environment

1. **Create virtual environment**
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

2. **Install development dependencies**
   ```bash
   pip install -e ".[dev]"
   ```

3. **Install pre-commit hooks**
   ```bash
   pre-commit install
   ```

### Adding New Features

1. Create a new branch
2. Write tests first (TDD approach)
3. Implement the feature
4. Ensure all tests pass
5. Run linting and formatting
6. Create a pull request

## 📈 Monitoring and Alerts

### Streaming Alerts

The Spark streaming consumer monitors for flash sale opportunities:
- **Condition**: Views > 100 AND Purchases < 5 in a 10-minute window
- **Output**: Console logs with product details

Example alert:
```
+-------------------+-------------------+-----------+------+---------+
|window_start       |window_end         |product_id |views |purchases|
+-------------------+-------------------+-----------+------+---------+
|2024-01-01 10:00:00|2024-01-01 10:10:00|product_42 |150   |3        |
+-------------------+-------------------+-----------+------+---------+
```

### Batch Reports

Daily summaries include:
- Total events processed
- User segmentation (Buyers vs Window Shoppers)
- Top 5 viewed products
- Generated at timestamp

## 🐳 Docker Services

### Simulator Service
Continuously generates clickstream events and publishes to Kafka.

### Streaming Service
Spark application that consumes from Kafka and generates alerts.

### Airflow Services
- **Webserver**: Web UI for managing DAGs
- **Scheduler**: Schedules and triggers DAG runs
- **Init**: Initializes database and creates admin user

## 🔐 Security

⚠️ **IMPORTANT SECURITY NOTICE**: This project uses Apache Airflow 2.10.4 which has a known **HIGH severity vulnerability** (proxy credentials leak) that **CANNOT be fixed** with currently available versions. The patched version (3.1.6) does not exist yet.

**Options**:
1. Accept the risk (mitigations applied) - see [SECURITY.md](SECURITY.md)
2. Do not use the Airflow batch component until 3.1.6 is released
3. Use alternative batch scheduler (see documentation)

**Other Security Measures**:
- 4 out of 5 critical Airflow CVEs fixed (latest stable version)
- Secrets managed via environment variables
- No hardcoded credentials
- Docker network isolation
- Regular dependency security scans in CI
- Security configuration hardening applied

**For full details**, see [SECURITY.md](SECURITY.md)

## 📝 License

This project is licensed under the MIT License.

## 👥 Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

## 🐛 Troubleshooting

### Kafka Connection Issues
- Ensure Kafka is running: `docker-compose ps kafka`
- Check Kafka logs: `docker-compose logs kafka`
- Verify network connectivity: `docker network inspect clickstream-network`

### Spark Streaming Issues
- Check Spark Master UI: http://localhost:8080
- View streaming logs: `docker-compose logs streaming`
- Verify checkpoint directory permissions

### Airflow Issues
- Access Airflow UI: http://localhost:8081
- Check scheduler logs: `docker-compose logs airflow-scheduler`
- Verify DAG is not paused in the UI

## 📚 Additional Resources

- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [Apache Spark Structured Streaming](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
- [Apache Airflow Documentation](https://airflow.apache.org/docs/)

## 📞 Support

For issues, questions, or contributions, please open an issue on GitHub.