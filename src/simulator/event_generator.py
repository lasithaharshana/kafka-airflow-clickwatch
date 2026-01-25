"""Clickstream event simulator."""

import json
import logging
import random
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Optional

from kafka import KafkaProducer

from config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
    NUM_PRODUCTS,
    NUM_USERS,
    RAW_LOGS_PATH,
    SIMULATOR_INTERVAL,
)

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class ClickstreamSimulator:
    """Simulate e-commerce clickstream events."""

    EVENT_TYPES = ["view", "add_to_cart", "purchase"]
    # Event type probabilities: view is most common, purchase is least common
    EVENT_WEIGHTS = [0.7, 0.2, 0.1]
    
    # Product categories for an electronics store
    PRODUCT_CATEGORIES = [
        "smartphones",
        "laptops",
        "tablets",
        "headphones",
        "smartwatches",
        "cameras",
        "gaming",
        "accessories",
    ]
    
    # Map products to categories (product_1 to product_100 distributed across categories)
    def _get_product_category(self, product_id: str) -> str:
        """Get the category for a product based on its ID."""
        try:
            product_num = int(product_id.split("_")[1])
            category_idx = product_num % len(self.PRODUCT_CATEGORIES)
            return self.PRODUCT_CATEGORIES[category_idx]
        except (IndexError, ValueError):
            return "accessories"

    def __init__(
        self,
        bootstrap_servers: str = KAFKA_BOOTSTRAP_SERVERS,
        topic: str = KAFKA_TOPIC,
        num_users: int = NUM_USERS,
        num_products: int = NUM_PRODUCTS,
        interval: float = SIMULATOR_INTERVAL,
        logs_path: str = RAW_LOGS_PATH,
    ):
        """Initialize the simulator.

        Args:
            bootstrap_servers: Kafka bootstrap servers
            topic: Kafka topic to publish events
            num_users: Number of unique users to simulate
            num_products: Number of unique products to simulate
            interval: Time interval between events in seconds
            logs_path: Path to write log files for batch processing
        """
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.num_users = num_users
        self.num_products = num_products
        self.interval = interval
        self.logs_path = Path(logs_path)
        self.producer = None
        self._current_log_file = None
        self._current_log_date = None

    def _initialize_producer(self, max_retries: int = 30, retry_delay: int = 5) -> None:
        """Initialize Kafka producer with retry logic.
        
        Args:
            max_retries: Maximum number of connection attempts
            retry_delay: Delay between retries in seconds
        """
        for attempt in range(max_retries):
            try:
                self.producer = KafkaProducer(
                    bootstrap_servers=self.bootstrap_servers,
                    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                )
                logger.info(f"Connected to Kafka at {self.bootstrap_servers}")
                return
            except Exception as e:
                logger.warning(
                    f"Failed to connect to Kafka (attempt {attempt + 1}/{max_retries}): {e}"
                )
                if attempt < max_retries - 1:
                    logger.info(f"Retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                else:
                    logger.error("Max retries reached. Could not connect to Kafka.")
                    raise

    def generate_event(self) -> Dict[str, str]:
        """Generate a single clickstream event.

        Returns:
            Dictionary containing event data with keys: user_id, product_id,
            product_category, event_type, timestamp
        """
        user_id = f"user_{random.randint(1, self.num_users)}"
        product_id = f"product_{random.randint(1, self.num_products)}"
        product_category = self._get_product_category(product_id)
        event_type = random.choices(self.EVENT_TYPES, weights=self.EVENT_WEIGHTS)[0]
        timestamp = datetime.now(timezone.utc).isoformat()

        event = {
            "user_id": user_id,
            "product_id": product_id,
            "product_category": product_category,
            "event_type": event_type,
            "timestamp": timestamp,
        }
        return event

    def send_event(self, event: Dict[str, str]) -> None:
        """Send event to Kafka.

        Args:
            event: Event dictionary to send
        """
        try:
            future = self.producer.send(self.topic, value=event)
            future.get(timeout=10)
            logger.debug(f"Sent event: {event}")
        except Exception as e:
            logger.error(f"Failed to send event: {e}")

    def _get_log_file(self) -> Optional[Path]:
        """Get the current log file, rotating daily.

        Returns:
            Path to the current log file
        """
        today = datetime.now().strftime("%Y-%m-%d")
        
        if self._current_log_date != today:
            # Close existing file if open
            if self._current_log_file:
                self._current_log_file.close()
            
            # Create logs directory if it doesn't exist
            self.logs_path.mkdir(parents=True, exist_ok=True)
            
            # Open new log file
            log_file_path = self.logs_path / f"clickstream_{today}.jsonl"
            self._current_log_file = open(log_file_path, "a")
            self._current_log_date = today
            logger.info(f"Writing to log file: {log_file_path}")
        
        return self._current_log_file

    def write_to_log(self, event: Dict[str, str]) -> None:
        """Write event to log file for batch processing.

        Args:
            event: Event dictionary to write
        """
        try:
            log_file = self._get_log_file()
            if log_file:
                log_file.write(json.dumps(event) + "\n")
                log_file.flush()
        except Exception as e:
            logger.error(f"Failed to write event to log: {e}")

    def run(self, duration: int = None) -> None:
        """Run the simulator.

        Args:
            duration: Duration to run in seconds. If None, runs indefinitely.
        """
        self._initialize_producer()
        
        logger.info("=" * 60)
        logger.info("CLICKSTREAM EVENT SIMULATOR STARTED")
        logger.info("=" * 60)
        logger.info(f"  Kafka Topic     : {self.topic}")
        logger.info(f"  Kafka Servers   : {self.bootstrap_servers}")
        logger.info(f"  Simulated Users : {self.num_users}")
        logger.info(f"  Products        : {self.num_products}")
        logger.info(f"  Event Interval  : {self.interval}s")
        logger.info(f"  Log Directory   : {self.logs_path}")
        logger.info("=" * 60)

        start_time = time.time()
        event_count = 0
        event_stats = {"view": 0, "add_to_cart": 0, "purchase": 0}

        try:
            while True:
                if duration and (time.time() - start_time) > duration:
                    break

                event = self.generate_event()
                self.send_event(event)
                self.write_to_log(event)
                event_count += 1
                event_stats[event["event_type"]] += 1

                # Log individual events at DEBUG level
                logger.debug(
                    f"EVENT: {event['event_type']:12} | "
                    f"User: {event['user_id']:10} | "
                    f"Product: {event['product_id']:12} | "
                    f"Category: {event['product_category']}"
                )

                # Log summary every 100 events
                if event_count % 100 == 0:
                    elapsed = time.time() - start_time
                    rate = event_count / elapsed if elapsed > 0 else 0
                    logger.info(
                        f"[PROGRESS] Events: {event_count:,} | "
                        f"Rate: {rate:.1f}/sec | "
                        f"Views: {event_stats['view']:,} | "
                        f"Carts: {event_stats['add_to_cart']:,} | "
                        f"Purchases: {event_stats['purchase']:,}"
                    )

                time.sleep(self.interval)

        except KeyboardInterrupt:
            logger.info("Simulator stopped by user")
        finally:
            if self.producer:
                self.producer.flush()
                self.producer.close()
            if self._current_log_file:
                self._current_log_file.close()
            
            logger.info("=" * 60)
            logger.info("SIMULATOR STOPPED - FINAL SUMMARY")
            logger.info("=" * 60)
            logger.info(f"  Total Events    : {event_count:,}")
            logger.info(f"  Views           : {event_stats['view']:,} ({event_stats['view']/max(event_count,1)*100:.1f}%)")
            logger.info(f"  Add to Cart     : {event_stats['add_to_cart']:,} ({event_stats['add_to_cart']/max(event_count,1)*100:.1f}%)")
            logger.info(f"  Purchases       : {event_stats['purchase']:,} ({event_stats['purchase']/max(event_count,1)*100:.1f}%)")
            logger.info("=" * 60)


def main():
    """Run the simulator."""
    simulator = ClickstreamSimulator()
    simulator.run()


if __name__ == "__main__":
    main()
