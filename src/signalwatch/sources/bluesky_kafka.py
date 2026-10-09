"""
Prepare Bluesky Jetstream post events for Kafka.

Validates essential routing metadata, generates a stable Kafka message
key, and preserves the original Jetstream JSON as the message value.
This module performs no network or database operations.
"""

import json
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class KafkaPostMessage:
    """A validated Bluesky event ready to publish to Kafka."""

    key: str
    value: bytes
    time_us: int
    operation: str


def prepare_post_message(
    event: dict[str, Any],
) -> KafkaPostMessage | None:
    """
    Convert a Jetstream post commit into a Kafka message.

    Returns None for account, identity, or unrelated collection events.
    Raises ValueError if a selected post event lacks essential metadata.
    """

    if event.get("kind") != "commit":
        return None

    commit = event.get("commit")

    if not isinstance(commit, dict):
        raise ValueError("Jetstream commit is missing or invalid.")

    if commit.get("collection") != "app.bsky.feed.post":
        return None

    did = event.get("did")
    rkey = commit.get("rkey")
    time_us = event.get("time_us")
    operation = commit.get("operation")

    if not isinstance(did, str) or not did:
        raise ValueError("Post event is missing a valid DID.")

    if not isinstance(rkey, str) or not rkey:
        raise ValueError("Post event is missing a valid record key.")

    if type(time_us) is not int or time_us <= 0:
        raise ValueError("Post event has an invalid timestamp.")

    if operation not in {"create", "update", "delete"}:
        raise ValueError(f"Unsupported post operation: {operation}")

    # All operations on the same post use the same Kafka key.
    post_uri = f"at://{did}/app.bsky.feed.post/{rkey}"

    # Store the complete source event, not a lossy normalized subset.
    payload = json.dumps(
        event,
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")

    return KafkaPostMessage(
        key=post_uri,
        value=payload,
        time_us=time_us,
        operation=operation,
    )
