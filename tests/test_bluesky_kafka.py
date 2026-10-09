"""
Unit tests for Bluesky-to-Kafka event preparation.

Verifies stable message keys, preservation of raw source payloads,
handling of delete operations, filtering, and basic validation.
No running Kafka broker is required.
"""

import json

import pytest

from signalwatch.sources.bluesky_kafka import prepare_post_message


def make_post_event(
    operation: str = "create",
) -> dict:
    """Build a minimal valid Bluesky Jetstream post event."""

    event = {
        "did": "did:plc:example123",
        "time_us": 1725911162329308,
        "kind": "commit",
        "commit": {
            "collection": "app.bsky.feed.post",
            "operation": operation,
            "rkey": "post123",
        },
    }

    if operation != "delete":
        event["commit"]["record"] = {
            "text": "Real-time data engineering",
            "langs": ["en"],
        }

    return event


def test_prepare_post_message() -> None:
    """A valid create operation should become a Kafka message."""

    original = make_post_event()
    message = prepare_post_message(original)

    assert message is not None
    assert message.operation == "create"
    assert message.time_us == original["time_us"]

    assert message.key == ("at://did:plc:example123/app.bsky.feed.post/post123")

    assert json.loads(message.value) == original


def test_updates_and_deletes_use_same_key() -> None:
    """All operations on the same post should share one Kafka key."""

    keys = [
        prepare_post_message(make_post_event(operation)).key
        for operation in ("create", "update", "delete")
    ]

    assert len(set(keys)) == 1


def test_delete_does_not_need_record() -> None:
    """A post deletion should publish without a record body."""

    message = prepare_post_message(make_post_event("delete"))

    assert message is not None
    assert message.operation == "delete"


def test_ignore_other_collections() -> None:
    """Non-post commits should not enter the post topic."""

    event = make_post_event()
    event["commit"]["collection"] = "app.bsky.feed.like"

    assert prepare_post_message(event) is None


def test_ignore_identity_event() -> None:
    """Identity updates are outside this S2 topic's scope."""

    assert (
        prepare_post_message(
            {
                "kind": "identity",
                "did": "did:plc:example123",
            }
        )
        is None
    )


def test_reject_invalid_timestamp() -> None:
    """An invalid Jetstream timestamp must not be checkpointed."""

    event = make_post_event()
    event["time_us"] = "invalid"

    with pytest.raises(ValueError, match="timestamp"):
        prepare_post_message(event)
