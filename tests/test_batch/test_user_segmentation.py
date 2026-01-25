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
