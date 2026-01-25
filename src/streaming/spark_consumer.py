"""Spark Structured Streaming consumer for clickstream analytics."""

import logging
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    from_json,
    current_timestamp,
)
from pyspark.sql.functions import sum as spark_sum
from pyspark.sql.functions import (
    window,
)
from pyspark.sql.types import StringType, StructField, StructType

from config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
    PURCHASES_THRESHOLD,
    SLIDE_DURATION,
    SPARK_APP_NAME,
    SPARK_MASTER,
    VIEWS_THRESHOLD,
    WINDOW_DURATION,
)

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Parquet output configuration
PARQUET_OUTPUT_PATH = os.getenv("PARQUET_OUTPUT_PATH", "/opt/spark/work-dir/data/parquet")
PARQUET_CHECKPOINT_PATH = os.getenv("PARQUET_CHECKPOINT_PATH", "/tmp/parquet_checkpoint")
ENABLE_PARQUET_SINK = os.getenv("ENABLE_PARQUET_SINK", "true").lower() == "true"

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class ClickstreamConsumer:
    """Consume and analyze clickstream events using Spark Structured Streaming."""

    def __init__(
        self,
        app_name: str = SPARK_APP_NAME,
        master: str = SPARK_MASTER,
        bootstrap_servers: str = KAFKA_BOOTSTRAP_SERVERS,
        topic: str = KAFKA_TOPIC,
    ):
        """Initialize the consumer.

        Args:
            app_name: Spark application name
            master: Spark master URL
            bootstrap_servers: Kafka bootstrap servers
            topic: Kafka topic to consume from
        """
        self.app_name = app_name
        self.master = master
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.spark = None

    def _create_spark_session(self) -> SparkSession:
        """Create and configure Spark session.

        Returns:
            SparkSession instance
        """
        spark = (
            SparkSession.builder.appName(self.app_name)
            .master(self.master)
            .config(
                "spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
            )
            .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("WARN")
        return spark

    def _define_schema(self) -> StructType:
        """Define the schema for clickstream events.

        Returns:
            StructType schema
        """
        return StructType(
            [
                StructField("user_id", StringType(), True),
                StructField("product_id", StringType(), True),
                StructField("product_category", StringType(), True),
                StructField("event_type", StringType(), True),
                StructField("timestamp", StringType(), True),
            ]
        )

    def process_stream(self):
        """Process clickstream events and detect flash sale opportunities."""
        self.spark = self._create_spark_session()
        
        logger.info("=" * 60)
        logger.info("SPARK STREAMING CONSUMER STARTED")
        logger.info("=" * 60)
        logger.info(f"  Spark App       : {self.app_name}")
        logger.info(f"  Spark Master    : {self.master}")
        logger.info(f"  Kafka Topic     : {self.topic}")
        logger.info(f"  Kafka Servers   : {self.bootstrap_servers}")
        logger.info(f"  Window Duration : {WINDOW_DURATION}")
        logger.info(f"  Slide Duration  : {SLIDE_DURATION}")
        logger.info("=" * 60)
        logger.info("FLASH SALE DETECTION CRITERIA:")
        logger.info(f"  - Product views    > {VIEWS_THRESHOLD}")
        logger.info(f"  - Product purchases < {PURCHASES_THRESHOLD}")
        logger.info("=" * 60)

        # Define schema
        schema = self._define_schema()

        # Read from Kafka
        logger.info("Connecting to Kafka stream...")
        df = (
            self.spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", self.bootstrap_servers)
            .option("subscribe", self.topic)
            .option("startingOffsets", "latest")
            .load()
        )
        logger.info("Connected to Kafka successfully")

        # Parse JSON data
        events_df = df.select(
            from_json(col("value").cast("string"), schema).alias("data")
        ).select("data.*")

        # Convert timestamp to proper format
        events_df = events_df.withColumn(
            "event_timestamp", col("timestamp").cast("timestamp")
        )

        # Windowed aggregation
        windowed_stats = events_df.groupBy(
            window(col("event_timestamp"), WINDOW_DURATION, SLIDE_DURATION),
            col("product_id"),
        ).agg(
            spark_sum((col("event_type") == "view").cast("int")).alias("views"),
            spark_sum((col("event_type") == "purchase").cast("int")).alias("purchases"),
            count("*").alias("total_events"),
        )

        # Filter for flash sale opportunities
        flash_sale_alerts = windowed_stats.filter(
            (col("views") > VIEWS_THRESHOLD) & (col("purchases") < PURCHASES_THRESHOLD)
        ).select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("product_id"),
            col("views"),
            col("purchases"),
        )

        # Write to console
        console_query = (
            flash_sale_alerts.writeStream.outputMode("update")
            .format("console")
            .option("truncate", False)
            .trigger(processingTime="30 seconds")
            .start()
        )

        # Write aggregated stats to Parquet (for historical analysis)
        parquet_query = None
        if ENABLE_PARQUET_SINK:
            logger.info(f"Enabling Parquet sink at: {PARQUET_OUTPUT_PATH}")
            
            # Add processing timestamp for partitioning
            parquet_df = windowed_stats.select(
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
                col("product_id"),
                col("views"),
                col("purchases"),
                col("total_events"),
                current_timestamp().alias("processed_at"),
            )
            
            parquet_query = (
                parquet_df.writeStream
                .outputMode("append")
                .format("parquet")
                .option("path", PARQUET_OUTPUT_PATH)
                .option("checkpointLocation", PARQUET_CHECKPOINT_PATH)
                .trigger(processingTime="60 seconds")
                .start()
            )
            logger.info("Parquet sink started - Writing windowed aggregations to Parquet")

        logger.info("=" * 60)
        logger.info("STREAMING QUERY ACTIVE - Monitoring for flash sales...")
        logger.info("Products with high views but low purchases will be shown below")
        if ENABLE_PARQUET_SINK:
            logger.info(f"Parquet output: {PARQUET_OUTPUT_PATH}")
        logger.info("=" * 60)

        try:
            console_query.awaitTermination()
        except KeyboardInterrupt:
            logger.info("Streaming stopped by user")
        finally:
            if parquet_query:
                parquet_query.stop()
            if self.spark:
                self.spark.stop()
            logger.info("Spark session closed")


def main():
    """Run the streaming consumer."""
    consumer = ClickstreamConsumer()
    consumer.process_stream()


if __name__ == "__main__":
    main()
