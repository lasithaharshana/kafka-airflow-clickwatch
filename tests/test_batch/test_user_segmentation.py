"""Tests for user segmentation."""

import json
import tempfile
from datetime import datetime, timedelta
from pathlib import Path

import pytest

from src.batch.user_segmentation import UserSegmentation


@pytest.fixture
def sample_events():
    """Create sample events for testing."""
    return [
        {
            "user_id": "user_1",
            "product_id": "product_1",
            "event_type": "view",
            "timestamp": "2024-01-01T10:00:00",
        },
        {
            "user_id": "user_1",
            "product_id": "product_1",
            "event_type": "purchase",
            "timestamp": "2024-01-01T10:05:00",
        },
        {
            "user_id": "user_2",
            "product_id": "product_2",
            "event_type": "view",
            "timestamp": "2024-01-01T10:10:00",
        },
        {
            "user_id": "user_2",
            "product_id": "product_2",
            "event_type": "add_to_cart",
            "timestamp": "2024-01-01T10:15:00",
        },
        {
            "user_id": "user_3",
            "product_id": "product_1",
            "event_type": "view",
            "timestamp": "2024-01-01T10:20:00",
        },
        {
            "user_id": "user_3",
            "product_id": "product_3",
            "event_type": "view",
            "timestamp": "2024-01-01T10:25:00",
        },
    ]


@pytest.fixture
def temp_log_dir(sample_events):
    """Create temporary log directory with sample data."""
    with tempfile.TemporaryDirectory() as tmpdir:
        log_path = Path(tmpdir)
        date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        log_file = log_path / f"clickstream_{date}.jsonl"

        with open(log_file, "w") as f:
            for event in sample_events:
                f.write(json.dumps(event) + "\n")

        yield str(log_path)


def test_segment_users(sample_events):
    """Test user segmentation logic."""
    segmentation = UserSegmentation("/tmp/logs")
    window_shoppers, buyers = segmentation.segment_users(sample_events)

    assert "user_1" in buyers
    assert "user_2" in window_shoppers
    assert "user_3" in window_shoppers
    assert len(buyers) == 1
    assert len(window_shoppers) == 2


def test_get_top_viewed_products(sample_events):
    """Test getting top viewed products."""
    segmentation = UserSegmentation("/tmp/logs")
    top_products = segmentation.get_top_viewed_products(sample_events, top_n=5)

    assert len(top_products) <= 5
    assert top_products[0][0] == "product_1"  # Most viewed
    assert top_products[0][1] == 2  # 2 views


def test_read_logs(temp_log_dir):
    """Test reading logs from file."""
    segmentation = UserSegmentation(temp_log_dir)
    date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    events = segmentation.read_logs(date)

    assert len(events) == 6
    assert all("user_id" in e for e in events)


def test_calculate_conversion_rates(sample_events):
    """Test calculating conversion rates by category."""
    # Add product_category to sample events
    events_with_category = []
    for event in sample_events:
        event_copy = event.copy()
        event_copy["product_category"] = "electronics" if event["product_id"] == "product_1" else "books"
        events_with_category.append(event_copy)

    segmentation = UserSegmentation("/tmp/logs")
    rates = segmentation.calculate_conversion_rates(events_with_category)

    assert "electronics" in rates
    assert "books" in rates
    assert rates["electronics"]["views"] == 2  # user_1 view and purchase, user_3 view
    assert rates["electronics"]["purchases"] == 1  # user_1 purchase
    assert rates["electronics"]["conversion_rate"] == 50.0  # 1/2 * 100


def test_calculate_conversion_rates_no_views():
    """Test conversion rates when there are no views."""
    events = [
        {
            "user_id": "user_1",
            "product_id": "product_1",
            "event_type": "purchase",
            "timestamp": "2024-01-01T10:00:00",
            "product_category": "electronics",
        }
    ]
    segmentation = UserSegmentation("/tmp/logs")
    rates = segmentation.calculate_conversion_rates(events)

    assert rates["electronics"]["conversion_rate"] == 0.0


def test_generate_summary(temp_log_dir):
    """Test generating full summary report."""
    segmentation = UserSegmentation(temp_log_dir)
    date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    summary = segmentation.generate_summary(date)

    assert summary["date"] == date
    assert summary["total_events"] == 6
    assert summary["buyers"]["count"] == 1
    assert summary["window_shoppers"]["count"] == 2
    assert len(summary["top_5_viewed_products"]) == 3  # 3 unique products
    assert "conversion_rates_by_category" in summary
    assert "generated_at" in summary


def test_generate_summary_no_events(temp_log_dir):
    """Test generating summary when no events exist."""
    segmentation = UserSegmentation(temp_log_dir)
    # Use a date with no log file
    future_date = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")
    summary = segmentation.generate_summary(future_date)

    assert summary == {}


def test_generate_email_text(sample_events):
    """Test generating email text from summary."""
    # Create a mock summary
    summary = {
        "date": "2024-01-01",
        "total_events": 6,
        "buyers": {"count": 1},
        "window_shoppers": {"count": 2},
        "top_5_viewed_products": [
            {"product_id": "product_1", "views": 2},
            {"product_id": "product_2", "views": 1},
            {"product_id": "product_3", "views": 1},
        ],
        "conversion_rates_by_category": {
            "electronics": {"views": 2, "purchases": 1, "conversion_rate": 50.0},
            "books": {"views": 1, "purchases": 0, "conversion_rate": 0.0},
        },
        "generated_at": "2024-01-01T12:00:00",
    }

    segmentation = UserSegmentation("/tmp/logs")
    email_text = segmentation.generate_email_text(summary)

    assert "Daily E-commerce Summary - 2024-01-01" in email_text
    assert "Total Events: 6" in email_text
    assert "Buyers: 1" in email_text
    assert "Window Shoppers: 2" in email_text
    assert "product_1 - 2 views" in email_text
    assert "electronics" in email_text
    assert "50.00%" in email_text


def test_generate_email_text_empty_summary():
    """Test generating email text with empty summary."""
    segmentation = UserSegmentation("/tmp/logs")
    email_text = segmentation.generate_email_text({})

    assert email_text == "No data available for summary."


def test_segment_users_edge_cases():
    """Test user segmentation with edge cases."""
    segmentation = UserSegmentation("/tmp/logs")

    # Empty events
    window_shoppers, buyers = segmentation.segment_users([])
    assert window_shoppers == []
    assert buyers == []

    # Only purchases
    events = [
        {"user_id": "user_1", "event_type": "purchase"},
        {"user_id": "user_2", "event_type": "purchase"},
    ]
    window_shoppers, buyers = segmentation.segment_users(events)
    assert len(buyers) == 2
    assert window_shoppers == []

    # Only views
    events = [
        {"user_id": "user_1", "event_type": "view"},
        {"user_id": "user_2", "event_type": "view"},
    ]
    window_shoppers, buyers = segmentation.segment_users(events)
    assert len(window_shoppers) == 2
    assert buyers == []


def test_get_top_viewed_products_edge_cases(sample_events):
    """Test getting top viewed products with edge cases."""
    segmentation = UserSegmentation("/tmp/logs")

    # Empty events
    top_products = segmentation.get_top_viewed_products([])
    assert top_products == []

    # No views
    events_no_views = [
        {"user_id": "user_1", "event_type": "purchase", "product_id": "product_1"}
    ]
    top_products = segmentation.get_top_viewed_products(events_no_views)
    assert top_products == []

    # Top N larger than available
    top_products = segmentation.get_top_viewed_products(sample_events, top_n=10)
    assert len(top_products) == 3  # Only 3 products in sample


def test_generate_summary(temp_log_dir):
    """Test generating summary report."""
    segmentation = UserSegmentation(temp_log_dir)
    date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    summary = segmentation.generate_summary(date)

    assert "date" in summary
    assert "total_events" in summary
    assert "window_shoppers" in summary
    assert "buyers" in summary
    assert "top_5_viewed_products" in summary
    assert summary["total_events"] == 6


def test_generate_email_text(sample_events):
    """Test email text generation."""
    segmentation = UserSegmentation("/tmp/logs")
    window_shoppers, buyers = segmentation.segment_users(sample_events)
    top_products = segmentation.get_top_viewed_products(sample_events)

    summary = {
        "date": "2024-01-01",
        "total_events": len(sample_events),
        "window_shoppers": {"count": len(window_shoppers)},
        "buyers": {"count": len(buyers)},
        "top_5_viewed_products": [
            {"product_id": pid, "views": count} for pid, count in top_products
        ],
        "generated_at": datetime.now().isoformat(),
    }

    email_text = segmentation.generate_email_text(summary)

    assert "Daily E-commerce Summary" in email_text
    assert "2024-01-01" in email_text
    assert "product_1" in email_text
