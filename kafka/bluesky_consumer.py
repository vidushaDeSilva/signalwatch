"""
SignalWatch S2: Bluesky Kafka inspection consumer.

Subscribes to the Bluesky Kafka topic, decodes source JSON events,
logs sampled message metadata, and commits consumer offsets manually.

This is an inspection consumer only. Durable landing into files is
introduced in S3.
"""

import argparse
import json
import logging
import signal
import time
from types import FrameType

from confluent_kafka import Consumer, KafkaError, KafkaException

from signalwatch.logging_config import configure_logging
from signalwatch.settings import get_settings

logger = logging.getLogger("signalwatch.kafka.bluesky_consumer")

running = True


def request_shutdown(
    signum: int,
    frame: FrameType | None,
) -> None:
    """Ask the consumer loop to stop cleanly."""

    del frame

    global running
    running = False

    logger.info("Shutdown requested signal=%s", signum)


def main() -> None:
    """Read Kafka events and periodically commit processed offsets."""

    parser = argparse.ArgumentParser(description="Inspect real Bluesky events stored in Kafka")

    parser.add_argument(
        "--max-events",
        type=int,
        default=None,
        help="Stop after consuming the requested number of events.",
    )

    parser.add_argument(
        "--log-every",
        type=int,
        default=100,
        help="Log one event out of every N consumed events.",
    )

    args = parser.parse_args()

    if args.max_events is not None and args.max_events <= 0:
        parser.error("--max-events must be positive.")

    if args.log_every <= 0:
        parser.error("--log-every must be positive.")

    settings = get_settings()
    configure_logging(settings.log_level)

    consumer = Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "group.id": settings.kafka_bluesky_consumer_group,
            "client.id": "signalwatch-bluesky-inspector-s2",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
        }
    )

    signal.signal(signal.SIGINT, request_shutdown)
    signal.signal(signal.SIGTERM, request_shutdown)

    consumer.subscribe([settings.kafka_bluesky_topic])

    logger.info(
        "Consumer started topic=%s group=%s",
        settings.kafka_bluesky_topic,
        settings.kafka_bluesky_consumer_group,
    )

    total_consumed = 0
    uncommitted_count = 0
    last_commit_time = time.monotonic()

    try:
        while running:
            message = consumer.poll(timeout=1.0)

            if message is None:
                continue

            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    continue

                raise KafkaException(message.error())

            # Fail rather than silently skipping an unexpected bad event.
            # A proper quarantine workflow will be introduced in S7.
            event = json.loads(message.value().decode("utf-8"))

            if not isinstance(event, dict):
                raise ValueError("Kafka message is not a JSON object.")

            commit = event.get("commit", {})

            if not isinstance(commit, dict):
                raise ValueError("Invalid Bluesky commit payload.")

            key = message.key().decode("utf-8") if message.key() is not None else None

            total_consumed += 1

            if total_consumed == 1 or total_consumed % args.log_every == 0:
                logger.info(
                    "event_received count=%s operation=%s key=%s partition=%s offset=%s",
                    total_consumed,
                    commit.get("operation"),
                    key,
                    message.partition(),
                    message.offset(),
                )

            # Mark progress only after successful application handling.
            consumer.store_offsets(message=message)
            uncommitted_count += 1

            elapsed = time.monotonic() - last_commit_time

            if uncommitted_count >= settings.kafka_consumer_commit_every or elapsed >= 5:
                consumer.commit(asynchronous=False)

                logger.info(
                    "offsets_committed processed_since_commit=%s",
                    uncommitted_count,
                )

                uncommitted_count = 0
                last_commit_time = time.monotonic()

            if args.max_events is not None and total_consumed >= args.max_events:
                logger.info("Reached requested event limit.")
                break

    finally:
        try:
            if uncommitted_count:
                # Final commit on clean exit, including Ctrl+C.
                consumer.commit(asynchronous=False)
                logger.info(
                    "final_offsets_committed count=%s",
                    uncommitted_count,
                )
        finally:
            consumer.close()
            logger.info(
                "Consumer stopped total_consumed=%s",
                total_consumed,
            )


if __name__ == "__main__":
    main()
