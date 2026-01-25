"""Tests for clickstream simulator."""

from unittest.mock import MagicMock, patch

import pytest

from src.simulator.event_generator import ClickstreamSimulator


@pytest.fixture
def simulator():
    """Create a simulator instance for testing."""
    return ClickstreamSimulator(
        bootstrap_servers="localhost:9092",
        topic="test-topic",
        num_users=10,
        num_products=5,
        interval=0.01,
    )


def test_simulator_initialization(simulator):
    """Test simulator initializes with correct parameters."""
    assert simulator.bootstrap_servers == "localhost:9092"
    assert simulator.topic == "test-topic"
    assert simulator.num_users == 10
    assert simulator.num_products == 5
    assert simulator.interval == 0.01


def test_generate_event(simulator):
    """Test event generation."""
    event = simulator.generate_event()

    assert "user_id" in event
    assert "product_id" in event
    assert "event_type" in event
    assert "timestamp" in event

    assert event["event_type"] in ["view", "add_to_cart", "purchase"]
    assert event["user_id"].startswith("user_")
    assert event["product_id"].startswith("product_")


def test_event_type_distribution(simulator):
    """Test that event types follow expected distribution."""
    events = [simulator.generate_event() for _ in range(1000)]
    event_types = [e["event_type"] for e in events]

    view_count = event_types.count("view")
    cart_count = event_types.count("add_to_cart")
    purchase_count = event_types.count("purchase")

    # Views should be most common
    assert view_count > cart_count
    assert view_count > purchase_count


@patch("src.simulator.event_generator.KafkaProducer")
def test_send_event(mock_producer_class, simulator):
    """Test sending event to Kafka."""
    mock_producer = MagicMock()
    mock_future = MagicMock()
    mock_future.get.return_value = None
    mock_producer.send.return_value = mock_future
    mock_producer_class.return_value = mock_producer

    simulator._initialize_producer()

    event = {"user_id": "user_1", "product_id": "product_1", "event_type": "view"}
    simulator.send_event(event)

    mock_producer.send.assert_called_once_with("test-topic", value=event)


@patch("src.simulator.event_generator.KafkaProducer")
def test_run_with_duration(mock_producer_class, simulator):
    """Test running simulator with duration."""
    mock_producer = MagicMock()
    mock_future = MagicMock()
    mock_future.get.return_value = None
    mock_producer.send.return_value = mock_future
    mock_producer_class.return_value = mock_producer

    simulator.run(duration=1)

    assert mock_producer.send.call_count > 0
    mock_producer.flush.assert_called_once()
    mock_producer.close.assert_called_once()
