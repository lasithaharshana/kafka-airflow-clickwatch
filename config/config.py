"""Configuration settings for the e-commerce clickstream project."""

import os

from dotenv import load_dotenv

load_dotenv()

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "clickstream-events")

# Spark Configuration
SPARK_APP_NAME = os.getenv("SPARK_APP_NAME", "ClickstreamAnalytics")
SPARK_MASTER = os.getenv("SPARK_MASTER", "local[*]")

# Window Configuration
WINDOW_DURATION = os.getenv("WINDOW_DURATION", "10 minutes")
SLIDE_DURATION = os.getenv("SLIDE_DURATION", "5 minutes")

# Alert Thresholds
try:
    VIEWS_THRESHOLD = int(os.getenv("VIEWS_THRESHOLD", "100"))
    PURCHASES_THRESHOLD = int(os.getenv("PURCHASES_THRESHOLD", "5"))
except ValueError as e:
    raise ValueError(
        "Invalid threshold values in environment variables. "
        "VIEWS_THRESHOLD and PURCHASES_THRESHOLD must be integers."
    ) from e

# Data Paths
RAW_LOGS_PATH = os.getenv("RAW_LOGS_PATH", "/tmp/logs/clickstream")
OUTPUT_PATH = os.getenv("OUTPUT_PATH", "/tmp/output")

# Simulator Configuration
try:
    SIMULATOR_INTERVAL = float(os.getenv("SIMULATOR_INTERVAL", "0.1"))
    NUM_USERS = int(os.getenv("NUM_USERS", "1000"))
    NUM_PRODUCTS = int(os.getenv("NUM_PRODUCTS", "100"))
except ValueError as e:
    raise ValueError(
        "Invalid simulator configuration in environment variables. "
        "SIMULATOR_INTERVAL must be a float, "
        "NUM_USERS and NUM_PRODUCTS must be integers."
    ) from e
