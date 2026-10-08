"""
Tests for the shared SignalWatch configuration layer.

These tests verify that safe local defaults are available without requiring
external services such as Kafka.
"""

from signalwatch.settings import Settings


def test_default_kafka_bootstrap_server() -> None:
    """Local development should default to Kafka on localhost:9092."""

    settings = Settings(_env_file=None)

    assert settings.kafka_bootstrap_servers == "localhost:9092"


def test_default_smoke_topic() -> None:
    """S0 should use a dedicated topic that cannot be confused with real data."""

    settings = Settings(_env_file=None)

    assert settings.kafka_test_topic == "signalwatch.s0.smoke"
