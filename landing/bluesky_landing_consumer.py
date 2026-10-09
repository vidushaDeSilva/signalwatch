"""
Consume Bluesky Kafka events into validated JSONL landing batches.

Events are grouped by Kafka partition. Each batch is published
atomically into ready/ before the consumer commits its offsets.

This is a single-process local landing service. Cloud uploading
will be implemented separately in S4.
"""

import json
import logging
import signal
import time
from datetime import UTC, datetime
from pathlib import Path
from types import FrameType

from confluent_kafka import (
    Consumer,
    KafkaError,
    KafkaException,
    TopicPartition,
)

from landing.batch_writer import BatchWriter
from landing.cleanup import cleanup_landing
from signalwatch.logging_config import configure_logging
from signalwatch.settings import Settings, get_settings

logger = logging.getLogger("signalwatch.landing.consumer")


class BlueskyLandingConsumer:
    """Manage Kafka partition buffers and commit after file publication."""

    def __init__(self, settings: Settings) -> None:
        """Configure Kafka, file storage, and in-memory buffers."""

        self.settings = settings

        self.writer = BatchWriter(
            Path(settings.landing_dir),
            settings.landing_min_free_mb,
        )

        # Each partition has its own batch buffer.
        self.buffers: dict[int, list[dict]] = {}
        self.first_seen_at: dict[int, float] = {}

        self.running = True
        self.closing = False
        self.last_cleanup_at = time.monotonic()

        self.consumer = Consumer(
            {
                "bootstrap.servers": (settings.kafka_bootstrap_servers),
                "group.id": (settings.kafka_landing_consumer_group),
                "client.id": "signalwatch-bluesky-landing-s3",
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
                "enable.auto.offset.store": False,
                "max.poll.interval.ms": 300000,
            }
        )

        self.consumer.subscribe(
            [settings.kafka_bluesky_topic],
            on_revoke=self.on_revoke,
            on_lost=self.on_lost,
        )

    def request_shutdown(
        self,
        signum: int,
        frame: FrameType | None,
    ) -> None:
        """Request a graceful stop on Ctrl+C or termination."""

        del frame

        logger.info(
            "shutdown_requested signal=%s",
            signum,
        )

        self.running = False

    def on_revoke(
        self,
        consumer: Consumer,
        partitions: list[TopicPartition],
    ) -> None:
        """
        Flush and commit buffered records before a normal rebalance.

        Kafka may reassign a partition to another consumer.
        We must finish its pending work before relinquishing it.
        """

        del consumer

        if self.closing:
            return

        for partition in partitions:
            self.flush_partition(partition.partition)

    def on_lost(
        self,
        consumer: Consumer,
        partitions: list[TopicPartition],
    ) -> None:
        """
        Discard pending work if partition ownership is already lost.

        Uncommitted events can be replayed by the next owner.
        Do not attempt to commit offsets for lost partitions.
        """

        del consumer

        for partition in partitions:
            self.buffers.pop(partition.partition, None)
            self.first_seen_at.pop(partition.partition, None)

            logger.warning(
                "partition_lost partition=%s will_replay=true",
                partition.partition,
            )

    def process_message(self, message) -> None:
        """
        Validate a Kafka message and append it to its partition buffer.

        Malformed records stop the consumer rather than being
        silently committed. Quarantine handling comes in S7.
        """

        payload = message.value()

        if payload is None:
            raise ValueError("Kafka message is missing a value")

        raw_json = payload.decode("utf-8")
        event = json.loads(raw_json)

        if not isinstance(event, dict):
            raise ValueError("Bluesky event must be a JSON object")

        commit = event.get("commit")

        if (
            event.get("kind") != "commit"
            or not isinstance(commit, dict)
            or commit.get("collection") != "app.bsky.feed.post"
        ):
            raise ValueError("Kafka message is not a Bluesky post commit")

        source_time = event.get("time_us")

        if type(source_time) is not int or source_time <= 0:
            raise ValueError("Bluesky event has an invalid time_us")

        key = message.key()

        row = {
            "source": "bluesky",
            "kafka_topic": message.topic(),
            "kafka_partition": message.partition(),
            "kafka_offset": message.offset(),
            "kafka_key": (key.decode("utf-8") if key is not None else None),
            "source_time_us": source_time,
            "landed_at": datetime.now(UTC).isoformat(),
            "raw_json": raw_json,
        }

        partition = message.partition()
        buffer = self.buffers.setdefault(partition, [])

        if not buffer:
            self.first_seen_at[partition] = time.monotonic()

        buffer.append(row)

        if len(buffer) >= self.settings.landing_batch_size:
            self.flush_partition(partition)

    def flush_partition(self, partition: int) -> None:
        """
        Persist a partition batch and commit its last offset plus one.

        The Kafka offset is not committed until write_batch()
        returns a validated ready batch.
        """

        records = self.buffers.get(partition)

        if not records:
            return

        ready = self.writer.write_batch(records)

        committed = self.consumer.commit(
            offsets=[
                TopicPartition(
                    self.settings.kafka_bluesky_topic,
                    partition,
                    ready.next_offset,
                )
            ],
            asynchronous=False,
        )

        # Synchronous commits can still report per-partition errors.
        if not committed or any(item.error is not None for item in committed):
            raise RuntimeError(f"Kafka offset commit failed for partition {partition}")

        logger.info(
            "batch_committed partition=%s count=%s next_offset=%s path=%s",
            partition,
            ready.count,
            ready.next_offset,
            ready.path,
        )

        self.buffers.pop(partition, None)
        self.first_seen_at.pop(partition, None)

    def flush_due(self) -> None:
        """Publish partially filled batches after their time limit."""

        now = time.monotonic()

        for partition, started in list(self.first_seen_at.items()):
            if now - started >= self.settings.landing_flush_seconds:
                self.flush_partition(partition)

    def run(self) -> None:
        """Poll Kafka until shutdown or an unrecoverable error."""

        signal.signal(
            signal.SIGINT,
            self.request_shutdown,
        )

        signal.signal(
            signal.SIGTERM,
            self.request_shutdown,
        )

        # Only old abandoned temporary work and previously uploaded
        # archives are eligible for automatic cleanup.
        cleanup_landing(
            self.writer.root,
            self.settings.landing_staging_stale_minutes,
            self.settings.landing_archived_retention_days,
            apply=True,
        )

        logger.info(
            "landing_started topic=%s group=%s dir=%s",
            self.settings.kafka_bluesky_topic,
            self.settings.kafka_landing_consumer_group,
            self.writer.root,
        )

        clean_exit = False

        try:
            while self.running:
                message = self.consumer.poll(timeout=1.0)

                if message is not None:
                    if message.error():
                        if message.error().code() != KafkaError._PARTITION_EOF:
                            raise KafkaException(message.error())
                    else:
                        self.process_message(message)

                self.flush_due()

                # Cleanup is infrequent and does not touch ready/.
                if time.monotonic() - self.last_cleanup_at >= 3600:
                    cleanup_landing(
                        self.writer.root,
                        self.settings.landing_staging_stale_minutes,
                        self.settings.landing_archived_retention_days,
                        apply=True,
                    )

                    self.last_cleanup_at = time.monotonic()

            clean_exit = True

        finally:
            try:
                if clean_exit:
                    # Publish the final, partially filled batches.
                    for partition in list(self.buffers):
                        self.flush_partition(partition)

            finally:
                self.closing = True
                self.consumer.close()

                logger.info(
                    "landing_stopped clean=%s",
                    clean_exit,
                )


def main() -> None:
    """Configure logging and run the landing consumer."""

    settings = get_settings()
    configure_logging(settings.log_level)

    try:
        BlueskyLandingConsumer(settings).run()

    except Exception:
        logger.exception("Landing consumer failed; uncommitted records will replay")

        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
