"""PostgreSQL storage module for analytics data."""

import json
import logging
import os
from datetime import datetime
from typing import Dict, List, Optional

import psycopg2
from psycopg2.extras import execute_values

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


# Database configuration from environment
DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "postgres"),
    "port": os.getenv("POSTGRES_PORT", "5432"),
    "database": os.getenv("POSTGRES_DB", "analytics"),
    "user": os.getenv("POSTGRES_USER", "airflow"),
    "password": os.getenv("POSTGRES_PASSWORD", "airflow"),
}


class AnalyticsStorage:
    """Store and retrieve analytics data from PostgreSQL."""

    def __init__(self, db_config: Optional[Dict] = None):
        """Initialize storage with database configuration.
        
        Args:
            db_config: Database connection parameters
        """
        self.db_config = db_config or DB_CONFIG
        self.conn = None

    def connect(self) -> None:
        """Establish database connection."""
        try:
            self.conn = psycopg2.connect(**self.db_config)
            logger.info(f"Connected to PostgreSQL at {self.db_config['host']}")
        except Exception as e:
            logger.error(f"Failed to connect to PostgreSQL: {e}")
            raise

    def close(self) -> None:
        """Close database connection."""
        if self.conn:
            self.conn.close()
            logger.info("PostgreSQL connection closed")

    def initialize_schema(self) -> None:
        """Create analytics tables if they don't exist."""
        schema_sql = """
        -- Daily summary table
        CREATE TABLE IF NOT EXISTS daily_summary (
            id SERIAL PRIMARY KEY,
            report_date DATE NOT NULL UNIQUE,
            total_events INTEGER NOT NULL,
            buyer_count INTEGER NOT NULL,
            window_shopper_count INTEGER NOT NULL,
            buyer_rate DECIMAL(5,2),
            generated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        -- User segments table
        CREATE TABLE IF NOT EXISTS user_segments (
            id SERIAL PRIMARY KEY,
            report_date DATE NOT NULL,
            user_id VARCHAR(50) NOT NULL,
            segment VARCHAR(20) NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(report_date, user_id)
        );

        -- Product performance table
        CREATE TABLE IF NOT EXISTS product_performance (
            id SERIAL PRIMARY KEY,
            report_date DATE NOT NULL,
            product_id VARCHAR(50) NOT NULL,
            view_count INTEGER NOT NULL,
            rank INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(report_date, product_id)
        );

        -- Category conversion rates table
        CREATE TABLE IF NOT EXISTS category_conversion (
            id SERIAL PRIMARY KEY,
            report_date DATE NOT NULL,
            category VARCHAR(50) NOT NULL,
            views INTEGER NOT NULL,
            purchases INTEGER NOT NULL,
            conversion_rate DECIMAL(5,2) NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(report_date, category)
        );

        -- Create indexes for better query performance
        CREATE INDEX IF NOT EXISTS idx_daily_summary_date ON daily_summary(report_date);
        CREATE INDEX IF NOT EXISTS idx_user_segments_date ON user_segments(report_date);
        CREATE INDEX IF NOT EXISTS idx_product_performance_date ON product_performance(report_date);
        CREATE INDEX IF NOT EXISTS idx_category_conversion_date ON category_conversion(report_date);
        """
        
        try:
            with self.conn.cursor() as cur:
                cur.execute(schema_sql)
            self.conn.commit()
            logger.info("Analytics schema initialized successfully")
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Failed to initialize schema: {e}")
            raise

    def store_summary(self, summary: Dict) -> None:
        """Store daily summary in PostgreSQL.
        
        Args:
            summary: Summary dictionary from UserSegmentation
        """
        if not summary:
            logger.warning("Empty summary, nothing to store")
            return

        report_date = summary.get("date")
        if not report_date:
            logger.error("Summary missing 'date' field")
            return

        try:
            with self.conn.cursor() as cur:
                # Store daily summary
                buyer_count = summary.get("buyers", {}).get("count", 0)
                shopper_count = summary.get("window_shoppers", {}).get("count", 0)
                total_users = buyer_count + shopper_count
                buyer_rate = (buyer_count / total_users * 100) if total_users > 0 else 0

                cur.execute("""
                    INSERT INTO daily_summary 
                    (report_date, total_events, buyer_count, window_shopper_count, buyer_rate, generated_at)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (report_date) 
                    DO UPDATE SET 
                        total_events = EXCLUDED.total_events,
                        buyer_count = EXCLUDED.buyer_count,
                        window_shopper_count = EXCLUDED.window_shopper_count,
                        buyer_rate = EXCLUDED.buyer_rate,
                        generated_at = EXCLUDED.generated_at
                """, (
                    report_date,
                    summary.get("total_events", 0),
                    buyer_count,
                    shopper_count,
                    round(buyer_rate, 2),
                    summary.get("generated_at", datetime.now().isoformat())
                ))

                # Store top products
                top_products = summary.get("top_5_viewed_products", [])
                if top_products:
                    product_data = [
                        (report_date, p["product_id"], p["views"], idx + 1)
                        for idx, p in enumerate(top_products)
                    ]
                    execute_values(
                        cur,
                        """
                        INSERT INTO product_performance (report_date, product_id, view_count, rank)
                        VALUES %s
                        ON CONFLICT (report_date, product_id) 
                        DO UPDATE SET view_count = EXCLUDED.view_count, rank = EXCLUDED.rank
                        """,
                        product_data
                    )

                # Store conversion rates
                conversion_rates = summary.get("conversion_rates_by_category", {})
                if conversion_rates:
                    conversion_data = [
                        (report_date, category, stats["views"], stats["purchases"], stats["conversion_rate"])
                        for category, stats in conversion_rates.items()
                    ]
                    execute_values(
                        cur,
                        """
                        INSERT INTO category_conversion (report_date, category, views, purchases, conversion_rate)
                        VALUES %s
                        ON CONFLICT (report_date, category) 
                        DO UPDATE SET 
                            views = EXCLUDED.views, 
                            purchases = EXCLUDED.purchases, 
                            conversion_rate = EXCLUDED.conversion_rate
                        """,
                        conversion_data
                    )

            self.conn.commit()
            logger.info(f"Summary for {report_date} stored in PostgreSQL")

        except Exception as e:
            self.conn.rollback()
            logger.error(f"Failed to store summary: {e}")
            raise

    def store_user_segments(
        self, report_date: str, buyers: List[str], window_shoppers: List[str]
    ) -> None:
        """Store user segment data.
        
        Args:
            report_date: Date of the report
            buyers: List of buyer user IDs
            window_shoppers: List of window shopper user IDs
        """
        try:
            with self.conn.cursor() as cur:
                # Prepare data for bulk insert
                segment_data = []
                for user_id in buyers:
                    segment_data.append((report_date, user_id, "buyer"))
                for user_id in window_shoppers:
                    segment_data.append((report_date, user_id, "window_shopper"))

                if segment_data:
                    execute_values(
                        cur,
                        """
                        INSERT INTO user_segments (report_date, user_id, segment)
                        VALUES %s
                        ON CONFLICT (report_date, user_id) DO UPDATE SET segment = EXCLUDED.segment
                        """,
                        segment_data
                    )

            self.conn.commit()
            logger.info(f"Stored {len(segment_data)} user segments for {report_date}")

        except Exception as e:
            self.conn.rollback()
            logger.error(f"Failed to store user segments: {e}")
            raise

    def get_summary(self, report_date: str) -> Optional[Dict]:
        """Retrieve summary for a specific date.
        
        Args:
            report_date: Date to retrieve
            
        Returns:
            Summary dictionary or None
        """
        try:
            with self.conn.cursor() as cur:
                cur.execute("""
                    SELECT report_date, total_events, buyer_count, window_shopper_count, 
                           buyer_rate, generated_at
                    FROM daily_summary
                    WHERE report_date = %s
                """, (report_date,))
                
                row = cur.fetchone()
                if row:
                    return {
                        "date": str(row[0]),
                        "total_events": row[1],
                        "buyers": {"count": row[2]},
                        "window_shoppers": {"count": row[3]},
                        "buyer_rate": float(row[4]) if row[4] else 0,
                        "generated_at": str(row[5]) if row[5] else None
                    }
                return None

        except Exception as e:
            logger.error(f"Failed to retrieve summary: {e}")
            return None

    def get_conversion_trends(self, days: int = 7) -> List[Dict]:
        """Get conversion rate trends for the last N days.
        
        Args:
            days: Number of days to retrieve
            
        Returns:
            List of daily conversion data
        """
        try:
            with self.conn.cursor() as cur:
                cur.execute("""
                    SELECT report_date, category, conversion_rate
                    FROM category_conversion
                    WHERE report_date >= CURRENT_DATE - INTERVAL '%s days'
                    ORDER BY report_date DESC, conversion_rate DESC
                """, (days,))
                
                results = []
                for row in cur.fetchall():
                    results.append({
                        "date": str(row[0]),
                        "category": row[1],
                        "conversion_rate": float(row[2])
                    })
                return results

        except Exception as e:
            logger.error(f"Failed to retrieve conversion trends: {e}")
            return []


def init_analytics_db():
    """Initialize the analytics database schema."""
    storage = AnalyticsStorage()
    try:
        storage.connect()
        storage.initialize_schema()
        logger.info("Analytics database initialized successfully")
    finally:
        storage.close()


if __name__ == "__main__":
    init_analytics_db()
