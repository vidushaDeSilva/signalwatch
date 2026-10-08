"""
S0 Kafka smoke-test consumer.

This script reads JSON messages from the local SignalWatch smoke-test topic.
It demonstrates consumer groups, offsets, manual commits, message parsing,
and graceful shutdown.
"""

import json
import logging
import signal
from types import FrameType

from confluent_kafka import Consumer, KafkaError, KafkaException

from signalwatch.logging_config import configure_logging
from signalwatch.settings import get_settings

logger = logging.getLogger("signalwatch.kafka.consumer")

running = True


def handle_shutdown(
    signum: int,
    frame: FrameType | None,
) -> None:
    """Request a clean consumer shutdown after SIGINT or SIGTERM."""

    del frame

    global running
    running = False

    logger.info("Shutdown signal received signal=%s", signum)


def main() -> None:
    """Consume messages until the user stops the process."""

    settings = get_settings()
    configure_logging(settings.log_level)

    consumer = Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "group.id": settings.kafka_test_consumer_group,
            # For a brand-new consumer group, begin with existing messages.
            "auto.offset.reset": "earliest",
            # Commit only after our application has successfully handled a message.
            "enable.auto.commit": False,
        }
    )

    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)

    consumer.subscribe([settings.kafka_test_topic])

    logger.info(
        "Consumer started broker=%s topic=%s group=%s",
        settings.kafka_bootstrap_servers,
        settings.kafka_test_topic,
        settings.kafka_test_consumer_group,
    )

    try:
        while running:
            message = consumer.poll(timeout=1.0)

            if message is None:
                continue

            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    continue

                raise KafkaException(message.error())

            try:
                event = json.loads(message.value().decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError):
                # S0 only logs malformed data. A real DLQ is introduced later.
                logger.exception(
                    "Invalid message topic=%s partition=%s offset=%s",
                    message.topic(),
                    message.partition(),
                    message.offset(),
                )
                continue

            logger.info(
                "Consumed event_id=%s sequence=%s partition=%s offset=%s",
                event.get("event_id"),
                event.get("sequence"),
                message.partition(),
                message.offset(),
            )

            # Commit only after successful application-level handling.
            consumer.commit(message=message, asynchronous=False)

    finally:
        # close() also leaves the consumer group cleanly.
        consumer.close()
        logger.info("Consumer stopped cleanly.")


if __name__ == "__main__":
    main()
