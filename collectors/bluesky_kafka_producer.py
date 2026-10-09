"""
SignalWatch S2: live Bluesky-to-Kafka producer.

Reads public post operations from Bluesky Jetstream, publishes the
original JSON events to Kafka, and checkpoints source progress only
after successful Kafka delivery acknowledgements.

Reconnects after WebSocket interruptions. Kafka delivery failures
stop the process without advancing the failed batch's checkpoint.
"""

import argparse
import asyncio
import json
import logging
import random
import time
from pathlib import Path

from confluent_kafka import Producer
from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed, InvalidHandshake

from signalwatch.errors import KafkaPublishError
from signalwatch.logging_config import configure_logging
from signalwatch.settings import Settings, get_settings
from signalwatch.sources.bluesky import (
    build_subscription_url,
    load_cursor,
    save_cursor,
)
from signalwatch.sources.bluesky_kafka import prepare_post_message

logger = logging.getLogger("signalwatch.collectors.bluesky_kafka")


class BlueskyKafkaProducer:
    """Stream Bluesky post operations into a local Kafka topic."""

    def __init__(
        self,
        settings: Settings,
        max_events: int | None = None,
    ) -> None:
        """Initialize the producer and load its independent source cursor."""

        self.settings = settings
        self.max_events = max_events

        self.cursor_path = Path(settings.kafka_bluesky_cursor_file)
        self.cursor_us = load_cursor(self.cursor_path)

        self.producer = Producer(
            {
                "bootstrap.servers": settings.kafka_bootstrap_servers,
                "client.id": "signalwatch-bluesky-s2",
                "acks": "all",
                "enable.idempotence": True,
                "linger.ms": 20,
                "delivery.timeout.ms": 30000,
                "socket.keepalive.enable": True,
            }
        )

        self.pending_count = 0
        self.pending_cursor_us: int | None = None
        self.delivery_errors: list[str] = []
        self.acked_count = 0

        self.total_published = 0
        self.total_ignored = 0

        self.last_flush_time = time.monotonic()

    def on_delivery(self, error, message) -> None:
        """Record Kafka delivery successes and failures."""

        if error is not None:
            self.delivery_errors.append(str(error))
            logger.error("Kafka delivery failed: %s", error)
            return

        self.acked_count += 1

        if self.acked_count == 1:
            logger.info(
                "Kafka acknowledged message topic=%s partition=%s offset=%s",
                message.topic(),
                message.partition(),
                message.offset(),
            )

    def enqueue(self, event: dict) -> bool:
        """
        Queue one Bluesky post event for asynchronous Kafka delivery.

        Returns False for irrelevant source events.
        The source cursor is not saved here.
        """

        prepared = prepare_post_message(event)

        if prepared is None:
            self.total_ignored += 1
            return False

        # A Kafka message key keeps operations for one post together.
        self.producer.produce(
            topic=self.settings.kafka_bluesky_topic,
            key=prepared.key.encode("utf-8"),
            value=prepared.value,
            callback=self.on_delivery,
        )

        self.pending_count += 1

        self.pending_cursor_us = max(
            self.pending_cursor_us or 0,
            prepared.time_us,
        )

        # Serve any completed asynchronous delivery callbacks.
        self.producer.poll(0)

        if self.delivery_errors:
            raise KafkaPublishError(f"Kafka delivery failed: {self.delivery_errors[0]}")

        return True

    async def flush_batch(self) -> None:
        """
        Wait for every queued message to finish delivery.

        Checkpoint only if every message in the batch was acknowledged
        without error. On timeout/failure the saved cursor is unchanged.
        """

        if self.pending_count == 0:
            return

        expected_count = self.pending_count

        # flush() can block, so run it outside the asyncio event loop.
        remaining = await asyncio.to_thread(
            self.producer.flush,
            40,
        )

        if remaining != 0:
            raise KafkaPublishError(f"Kafka flush timed out with {remaining} messages pending.")

        if self.delivery_errors:
            raise KafkaPublishError(
                f"{len(self.delivery_errors)} Kafka delivery error(s): {self.delivery_errors[0]}"
            )

        if self.acked_count != expected_count:
            raise KafkaPublishError(
                "Kafka delivery acknowledgement count does not match the number of queued events."
            )

        if self.pending_cursor_us is None:
            raise KafkaPublishError("Missing pending source cursor.")

        # This is the recovery boundary: Kafka acknowledged the batch.
        save_cursor(self.cursor_path, self.pending_cursor_us)
        self.cursor_us = self.pending_cursor_us

        self.total_published += expected_count

        logger.info(
            "batch_delivered count=%s total=%s cursor_us=%s",
            expected_count,
            self.total_published,
            self.cursor_us,
        )

        self.pending_count = 0
        self.pending_cursor_us = None
        self.acked_count = 0
        self.delivery_errors.clear()
        self.last_flush_time = time.monotonic()

    async def run(self) -> None:
        """Consume live WebSocket events and publish them into Kafka."""

        reconnect_attempt = 0

        logger.info(
            "Starting Bluesky Kafka producer broker=%s topic=%s",
            self.settings.kafka_bootstrap_servers,
            self.settings.kafka_bluesky_topic,
        )

        try:
            while True:
                # Use only the last Kafka-acknowledged checkpoint.
                url = build_subscription_url(
                    base_url=self.settings.jetstream_url,
                    collection=self.settings.jetstream_collection,
                    cursor_us=self.cursor_us,
                    overlap_seconds=(self.settings.jetstream_replay_overlap_seconds),
                )

                connected_at = time.monotonic()
                last_message_at = time.monotonic()

                try:
                    logger.info("Connecting to Bluesky Jetstream")

                    async with connect(
                        url,
                        open_timeout=20,
                        close_timeout=10,
                        ping_interval=20,
                        ping_timeout=20,
                        max_size=2 * 1024 * 1024,
                        max_queue=16,
                        compression=None,
                    ) as websocket:
                        logger.info("Connected to Bluesky Jetstream")

                        while True:
                            try:
                                raw_message = await asyncio.wait_for(
                                    websocket.recv(),
                                    timeout=1,
                                )

                            except TimeoutError:
                                # Allow partially filled batches to finish.
                                elapsed = time.monotonic() - self.last_flush_time

                                if self.pending_count > 0 and elapsed >= (
                                    self.settings.kafka_producer_flush_seconds
                                ):
                                    await self.flush_batch()

                                if time.monotonic() - last_message_at > 90:
                                    raise TimeoutError("No Jetstream events for 90 seconds.")

                                continue

                            last_message_at = time.monotonic()

                            try:
                                event = json.loads(raw_message)

                                if not isinstance(event, dict):
                                    raise ValueError("Jetstream message is not an object.")

                                accepted = self.enqueue(event)

                            except (ValueError, UnicodeDecodeError) as exc:
                                logger.warning(
                                    "Skipping invalid source event: %s",
                                    exc,
                                )
                                continue

                            if not accepted:
                                continue

                            if self.pending_count >= self.settings.kafka_producer_batch_size:
                                await self.flush_batch()

                            elif (
                                time.monotonic() - self.last_flush_time
                                >= self.settings.kafka_producer_flush_seconds
                            ):
                                await self.flush_batch()

                            if (
                                self.max_events is not None
                                and self.total_published + self.pending_count >= self.max_events
                            ):
                                logger.info("Reached requested event limit.")
                                return

                except (
                    ConnectionClosed,
                    OSError,
                    TimeoutError,
                    InvalidHandshake,
                ) as exc:
                    logger.warning(
                        "Jetstream disconnected: %s",
                        exc,
                    )

                # Finish any queued batch before attempting source replay.
                await self.flush_batch()

                if time.monotonic() - connected_at >= 30:
                    reconnect_attempt = 0

                delay = min(60, 2 ** min(reconnect_attempt, 6))
                delay += random.uniform(0, 0.5)
                reconnect_attempt += 1

                logger.info(
                    "Reconnecting to Bluesky in %.1f seconds",
                    delay,
                )
                await asyncio.sleep(delay)

        finally:
            # A clean stop flushes the final partially filled batch.
            # Failed deliveries must never advance the checkpoint.
            await self.flush_batch()

            logger.info(
                "Producer stopped published=%s ignored=%s",
                self.total_published,
                self.total_ignored,
            )


def main() -> None:
    """Parse CLI arguments and start the live Kafka producer."""

    parser = argparse.ArgumentParser(description="SignalWatch Bluesky-to-Kafka producer")

    parser.add_argument(
        "--max-events",
        type=int,
        default=None,
        help="Stop after publishing this many Bluesky post events.",
    )

    args = parser.parse_args()

    if args.max_events is not None and args.max_events <= 0:
        parser.error("--max-events must be positive.")

    settings = get_settings()
    configure_logging(settings.log_level)

    publisher = BlueskyKafkaProducer(
        settings=settings,
        max_events=args.max_events,
    )

    try:
        asyncio.run(publisher.run())

    except KeyboardInterrupt:
        logger.info("Producer interrupted by user.")

    except KafkaPublishError:
        logger.exception("Kafka publish failed. Checkpoint was not advanced for the failed batch.")
        raise SystemExit(1)


if __name__ == "__main__":
    main()
