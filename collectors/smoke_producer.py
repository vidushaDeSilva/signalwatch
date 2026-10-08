"""
S0 Kafka smoke-test producer.

This script publishes a small set of JSON messages to the local Kafka broker.
It verifies that the Python environment, configuration system, Kafka producer,
and Docker broker can communicate successfully.
"""

import json
import logging
import sys
from datetime import UTC, datetime
from uuid import uuid4

from confluent_kafka import Producer

from signalwatch.errors import KafkaPublishError
from signalwatch.logging_config import configure_logging
from signalwatch.settings import get_settings

logger = logging.getLogger("signalwatch.kafka.producer")


def delivery_callback(error, message) -> None:
    """Log the final delivery result reported by Kafka."""

    if error is not None:
        logger.error("Kafka delivery failed: %s", error)
        return

    logger.info(
        "Message delivered topic=%s partition=%s offset=%s",
        message.topic(),
        message.partition(),
        message.offset(),
    )


def build_test_event(sequence: int) -> dict:
    """Create one small JSON event for the S0 connectivity test."""

    return {
        "event_id": str(uuid4()),
        "event_type": "s0_smoke_test",
        "sequence": sequence,
        "created_at": datetime.now(UTC).isoformat(),
        "message": f"SignalWatch Kafka smoke event {sequence}",
    }


def main() -> None:
    """Publish five test events and wait for delivery acknowledgements."""

    settings = get_settings()
    configure_logging(settings.log_level)

    producer = Producer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            # The producer receives an acknowledgement from the broker.
            "acks": "all",
        }
    )

    logger.info(
        "Starting smoke producer broker=%s topic=%s",
        settings.kafka_bootstrap_servers,
        settings.kafka_test_topic,
    )

    try:
        for sequence in range(1, 6):
            event = build_test_event(sequence)

            producer.produce(
                topic=settings.kafka_test_topic,
                # A stable key controls partition selection.
                key=event["event_id"],
                value=json.dumps(event).encode("utf-8"),
                callback=delivery_callback,
            )

            # Serve producer delivery callbacks without blocking indefinitely.
            producer.poll(0)

        remaining_messages = producer.flush(timeout=10)

        if remaining_messages:
            raise KafkaPublishError(f"{remaining_messages} Kafka message(s) were not delivered.")

        logger.info("Smoke producer completed successfully.")

    except BufferError as exc:
        logger.exception("Kafka producer queue is full.")
        raise KafkaPublishError("Kafka producer queue is full.") from exc

    except KafkaPublishError:
        raise

    except Exception as exc:
        logger.exception("Unexpected producer failure.")
        raise KafkaPublishError("Smoke producer failed.") from exc


if __name__ == "__main__":
    try:
        main()
    except KafkaPublishError as exc:
        logger.error("%s", exc)
        sys.exit(1)
