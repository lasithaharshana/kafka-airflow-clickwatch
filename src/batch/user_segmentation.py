"""User segmentation module for batch processing."""

import json
import logging
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class UserSegmentation:
    """Segment users into Window Shoppers and Buyers."""

    def __init__(self, logs_path: str):
        """Initialize user segmentation.

        Args:
            logs_path: Path to raw log files
        """
        self.logs_path = Path(logs_path)

    def read_logs(self, date: Optional[str] = None) -> List[Dict[str, str]]:
        """Read logs for a specific date.

        Args:
            date: Date in YYYY-MM-DD format. If None, uses yesterday.

        Returns:
            List of event dictionaries
        """
        if date is None:
            date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

        events = []
        log_file = self.logs_path / f"clickstream_{date}.jsonl"

        if not log_file.exists():
            logger.warning(f"Log file not found: {log_file}")
            return events

        try:
            with open(log_file, "r") as f:
                for line in f:
                    if line.strip():
                        events.append(json.loads(line))
            logger.info(f"Loaded {len(events)} events from {log_file}")
        except Exception as e:
            logger.error(f"Error reading logs: {e}")

        return events

    def segment_users(
        self, events: List[Dict[str, str]]
    ) -> Tuple[List[str], List[str]]:
        """Segment users into Window Shoppers and Buyers.

        Window Shoppers: Users who viewed/added to cart but never purchased
        Buyers: Users who made at least one purchase

        Args:
            events: List of event dictionaries

        Returns:
            Tuple of (window_shoppers, buyers) lists
        """
        user_actions = defaultdict(set)

        for event in events:
            user_id = event.get("user_id")
            event_type = event.get("event_type")
            if user_id and event_type:
                user_actions[user_id].add(event_type)

        buyers = [
            user_id
            for user_id, actions in user_actions.items()
            if "purchase" in actions
        ]
        window_shoppers = [
            user_id
            for user_id, actions in user_actions.items()
            if "purchase" not in actions and len(actions) > 0
        ]

        logger.info(
            f"Segmented users: {len(buyers)} buyers, "
            f"{len(window_shoppers)} window shoppers"
        )
        return window_shoppers, buyers

    def get_top_viewed_products(
        self, events: List[Dict[str, str]], top_n: int = 5
    ) -> List[Tuple[str, int]]:
        """Get top N most viewed products.

        Args:
            events: List of event dictionaries
            top_n: Number of top products to return

        Returns:
            List of (product_id, view_count) tuples
        """
        product_views = defaultdict(int)

        for event in events:
            if event.get("event_type") == "view":
                product_id = event.get("product_id")
                if product_id:
                    product_views[product_id] += 1

        top_products = sorted(product_views.items(), key=lambda x: x[1], reverse=True)[
            :top_n
        ]

        logger.info(f"Top {top_n} viewed products: {top_products}")
        return top_products

    def calculate_conversion_rates(
        self, events: List[Dict[str, str]]
    ) -> Dict[str, Dict[str, float]]:
        """Calculate conversion rates (Purchases/Views) per product category.

        Args:
            events: List of event dictionaries

        Returns:
            Dictionary with category as key and stats as value
        """
        category_stats = defaultdict(lambda: {"views": 0, "purchases": 0})

        for event in events:
            category = event.get("product_category", "unknown")
            event_type = event.get("event_type")
            
            if event_type == "view":
                category_stats[category]["views"] += 1
            elif event_type == "purchase":
                category_stats[category]["purchases"] += 1

        # Calculate conversion rates
        conversion_rates = {}
        for category, stats in category_stats.items():
            views = stats["views"]
            purchases = stats["purchases"]
            conversion_rate = (purchases / views * 100) if views > 0 else 0.0
            conversion_rates[category] = {
                "views": views,
                "purchases": purchases,
                "conversion_rate": round(conversion_rate, 2),
            }

        # Sort by conversion rate descending
        conversion_rates = dict(
            sorted(
                conversion_rates.items(),
                key=lambda x: x[1]["conversion_rate"],
                reverse=True,
            )
        )

        logger.info(f"Calculated conversion rates for {len(conversion_rates)} categories")
        return conversion_rates

    def generate_summary(
        self, date: Optional[str] = None, output_path: Optional[str] = None
    ) -> Dict:
        """Generate daily summary report.

        Args:
            date: Date in YYYY-MM-DD format
            output_path: Path to save summary file

        Returns:
            Summary dictionary
        """
        if date is None:
            date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

        logger.info("=" * 60)
        logger.info("BATCH PROCESSING - DAILY USER SEGMENTATION")
        logger.info("=" * 60)
        logger.info(f"  Processing Date : {date}")
        logger.info(f"  Logs Path       : {self.logs_path}")
        logger.info("=" * 60)

        events = self.read_logs(date)
        if not events:
            logger.warning(f"No events found for {date}")
            return {}

        logger.info(f"[STEP 1/4] Loaded {len(events):,} events from log file")

        window_shoppers, buyers = self.segment_users(events)
        logger.info(f"[STEP 2/4] User Segmentation Complete:")
        logger.info(f"           - Buyers         : {len(buyers):,}")
        logger.info(f"           - Window Shoppers: {len(window_shoppers):,}")

        top_products = self.get_top_viewed_products(events)
        logger.info(f"[STEP 3/4] Top Products Analysis Complete")

        conversion_rates = self.calculate_conversion_rates(events)
        logger.info(f"[STEP 4/4] Conversion Rates Calculated for {len(conversion_rates)} categories")

        summary = {
            "date": date,
            "total_events": len(events),
            "window_shoppers": {
                "count": len(window_shoppers),
                "user_ids": window_shoppers[:10],  # Sample of first 10
            },
            "buyers": {
                "count": len(buyers),
                "user_ids": buyers[:10],  # Sample of first 10
            },
            "top_5_viewed_products": [
                {"product_id": pid, "views": count} for pid, count in top_products
            ],
            "conversion_rates_by_category": conversion_rates,
            "generated_at": datetime.now().isoformat(),
        }

        # Save summary if output path is provided
        if output_path:
            output_file = Path(output_path) / f"summary_{date}.json"
            output_file.parent.mkdir(parents=True, exist_ok=True)
            with open(output_file, "w") as f:
                json.dump(summary, f, indent=2)
            logger.info(f"Summary saved to: {output_file}")

        logger.info("=" * 60)
        logger.info("BATCH PROCESSING COMPLETE - SUMMARY")
        logger.info("=" * 60)
        logger.info(f"  Total Events     : {len(events):,}")
        logger.info(f"  Unique Buyers    : {len(buyers):,}")
        logger.info(f"  Window Shoppers  : {len(window_shoppers):,}")
        logger.info(f"  Buyer Rate       : {len(buyers)/(len(buyers)+len(window_shoppers))*100:.1f}%")
        logger.info("=" * 60)

        return summary

    def generate_email_text(self, summary: Dict) -> str:
        """Generate email/text message from summary.

        Args:
            summary: Summary dictionary

        Returns:
            Formatted text message
        """
        if not summary:
            return "No data available for summary."

        text = f"""Daily E-commerce Summary - {summary['date']}
{'=' * 60}

Total Events: {summary['total_events']}

USER SEGMENTATION
{'-' * 40}
- Buyers: {summary['buyers']['count']}
- Window Shoppers: {summary['window_shoppers']['count']}

TOP 5 VIEWED PRODUCTS
{'-' * 40}
"""
        for i, product in enumerate(summary["top_5_viewed_products"], 1):
            text += f"{i}. {product['product_id']} - {product['views']} views\n"

        text += f"\nCONVERSION RATES BY PRODUCT CATEGORY\n{'-' * 40}\n"
        text += f"{'Category':<15} {'Views':>10} {'Purchases':>10} {'Rate':>10}\n"
        text += f"{'-' * 45}\n"
        
        conversion_rates = summary.get("conversion_rates_by_category", {})
        for category, stats in conversion_rates.items():
            text += f"{category:<15} {stats['views']:>10} {stats['purchases']:>10} {stats['conversion_rate']:>9.2f}%\n"

        text += f"\n{'=' * 60}\nReport generated at: {summary['generated_at']}"
        return text
