"""
SignalWatch S1: live Bluesky Jetstream collector.

Connects to the public Jetstream WebSocket, receives post events,
extracts basic metadata, logs samples, reconnects after failures,
and saves a cursor for restart recovery.

This sprint does not publish to Kafka or write analytical data.
"""

import argparse
import asyncio
import json
import logging
import random
import time
from datetime import UTC, datetime
from pathlib import Path

from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed, InvalidHandshake

from signalwatch.logging_config import configure_logging
from signalwatch.settings import Settings, get_settings
from signalwatch.sources.bluesky import (
    BlueskyPostEvent,
    build_subscription_url,
    load_cursor,
    parse_post_event,
    save_cursor,
)

logger = logging.getLogger("signalwatch.collectors.bluesky")


class BlueskyCollector:
    """Manage the live Jetstream connection and local recovery cursor."""

    def __init__(
        self,
        settings: Settings,
        max_posts: int | None = None,
        show_text: bool = False,
    ) -> None:
        """Initialize the collector using project configuration."""

        self.settings = settings
        self.max_posts = max_posts
        self.show_text = show_text

        self.cursor_path = Path(settings.jetstream_cursor_file)
        self.cursor_us = load_cursor(self.cursor_path)
        self.saved_cursor_us = self.cursor_us

        self.messages_seen = 0
        self.posts_seen = 0
        self.messages_since_save = 0
        self.last_save_time = time.monotonic()

    def log_post(self, event: BlueskyPostEvent) -> None:
        """Log a sample of post metadata rather than every incoming event."""

        if self.posts_seen != 1 and self.posts_seen % self.settings.jetstream_log_every != 0:
            return

        preview = ""

        if self.show_text and event.text:
            # Only show public post text when explicitly requested.
            short_text = " ".join(event.text.split())[:120]
            preview = f" text_preview={short_text!r}"

        logger.info(
            "post_received count=%s operation=%s uri=%s language=%s created_at=%s%s",
            self.posts_seen,
            event.operation,
            event.uri,
            event.language,
            event.created_at,
            preview,
        )

    def save_progress(self, force: bool = False) -> None:
        """Periodically persist the newest successfully handled cursor."""

        if self.cursor_us is None:
            return

        if self.cursor_us == self.saved_cursor_us:
            return

        elapsed = time.monotonic() - self.last_save_time

        should_save = (
            force
            or self.messages_since_save >= self.settings.jetstream_cursor_save_every
            or elapsed >= 5
        )

        if not should_save:
            return

        save_cursor(self.cursor_path, self.cursor_us)

        self.saved_cursor_us = self.cursor_us
        self.messages_since_save = 0
        self.last_save_time = time.monotonic()

    def update_progress(self, time_us: object) -> None:
        """Advance local progress using a valid Jetstream timestamp."""

        if type(time_us) is not int or time_us <= 0:
            return

        # Never move the saved position backwards.
        self.cursor_us = max(self.cursor_us or 0, time_us)

        self.messages_since_save += 1
        self.save_progress()

    def warn_if_cursor_old(self) -> None:
        """Warn if the saved cursor may exceed the live replay window."""

        if self.cursor_us is None:
            return

        age_hours = (datetime.now(UTC).timestamp() * 1_000_000 - self.cursor_us) / 3_600_000_000

        if age_hours > 30:
            logger.warning(
                "Saved cursor is approximately %.1f hours old. "
                "Jetstream v1 replay is bounded; some history may "
                "no longer be available.",
                age_hours,
            )

    async def run(self) -> None:
        """Receive public events and reconnect automatically when necessary."""

        self.warn_if_cursor_old()

        reconnect_attempt = 0

        try:
            while True:
                url = build_subscription_url(
                    base_url=self.settings.jetstream_url,
                    collection=self.settings.jetstream_collection,
                    cursor_us=self.cursor_us,
                    overlap_seconds=self.settings.jetstream_replay_overlap_seconds,
                )

                connection_started = time.monotonic()

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
                            # A silent connection is restarted instead
                            # of being allowed to remain stuck forever.
                            raw_message = await asyncio.wait_for(
                                websocket.recv(),
                                timeout=90,
                            )

                            self.messages_seen += 1

                            try:
                                data = json.loads(raw_message)

                                if not isinstance(data, dict):
                                    raise ValueError("Jetstream event is not a JSON object.")

                                post = parse_post_event(data)

                            except (ValueError, UnicodeDecodeError) as exc:
                                logger.warning(
                                    "Skipping invalid event: %s",
                                    exc,
                                )
                                continue

                            if post is not None:
                                self.posts_seen += 1
                                self.log_post(post)

                            # Advance only after handling the message.
                            self.update_progress(data.get("time_us"))

                            if self.max_posts is not None and self.posts_seen >= self.max_posts:
                                logger.info(
                                    "Reached requested limit of %s posts",
                                    self.max_posts,
                                )
                                return

                except (
                    ConnectionClosed,
                    OSError,
                    TimeoutError,
                    InvalidHandshake,
                ) as exc:
                    logger.warning(
                        "Jetstream connection interrupted: %s",
                        exc,
                    )

                else:
                    logger.warning("Jetstream connection closed.")

                # A stable connection resets the backoff sequence.
                if time.monotonic() - connection_started >= 30:
                    reconnect_attempt = 0

                delay = min(60.0, 2 ** min(reconnect_attempt, 6))
                delay += random.uniform(0, 0.5)
                reconnect_attempt += 1

                logger.info("Reconnecting in %.1f seconds", delay)
                await asyncio.sleep(delay)

        finally:
            # Save the last handled position before clean shutdown.
            self.save_progress(force=True)

            logger.info(
                "Collector stopped total_messages=%s total_posts=%s",
                self.messages_seen,
                self.posts_seen,
            )


def main() -> None:
    """Read command-line options and start the Bluesky collector."""

    parser = argparse.ArgumentParser(
        description="SignalWatch Bluesky live collector",
    )

    parser.add_argument(
        "--max-posts",
        type=int,
        default=None,
        help="Stop after receiving this many post operations.",
    )

    parser.add_argument(
        "--show-text",
        action="store_true",
        help="Include short public post previews in the logs.",
    )

    args = parser.parse_args()

    if args.max_posts is not None and args.max_posts <= 0:
        parser.error("--max-posts must be a positive integer.")

    settings = get_settings()
    configure_logging(settings.log_level)

    collector = BlueskyCollector(
        settings=settings,
        max_posts=args.max_posts,
        show_text=args.show_text,
    )

    try:
        asyncio.run(collector.run())

    except KeyboardInterrupt:
        logger.info("Collector interrupted by user.")


if __name__ == "__main__":
    main()
