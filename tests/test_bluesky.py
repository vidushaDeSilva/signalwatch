"""
Unit tests for the SignalWatch Bluesky event utilities.

Tests post creation, deletion, event filtering, cursor persistence,
and replay URL construction without making external API requests.
"""

from urllib.parse import parse_qs, urlsplit

import pytest

from signalwatch.sources.bluesky import (
    build_subscription_url,
    load_cursor,
    parse_post_event,
    save_cursor,
)


def sample_create_event() -> dict:
    """Return an example Jetstream post-creation event."""

    return {
        "did": "did:plc:example123",
        "time_us": 1725911162329308,
        "kind": "commit",
        "commit": {
            "operation": "create",
            "collection": "app.bsky.feed.post",
            "rkey": "post123",
            "record": {
                "text": "Testing SignalWatch",
                "createdAt": "2024-09-09T19:46:02Z",
                "langs": ["en"],
            },
        },
    }


def test_parse_post_creation() -> None:
    """Post creation should produce the expected metadata."""

    event = parse_post_event(sample_create_event())

    assert event is not None
    assert event.operation == "create"
    assert event.text == "Testing SignalWatch"
    assert event.language == "en"
    assert event.uri == "at://did:plc:example123/app.bsky.feed.post/post123"


def test_parse_post_deletion() -> None:
    """A deletion should be accepted without a post record."""

    data = sample_create_event()
    data["commit"]["operation"] = "delete"
    data["commit"].pop("record")

    event = parse_post_event(data)

    assert event is not None
    assert event.operation == "delete"
    assert event.text is None


def test_ignore_non_post_event() -> None:
    """Events outside the selected post collection should be ignored."""

    data = sample_create_event()
    data["commit"]["collection"] = "app.bsky.feed.like"

    assert parse_post_event(data) is None


def test_invalid_post_timestamp() -> None:
    """Invalid source timestamps should fail validation."""

    data = sample_create_event()
    data["time_us"] = "not-a-timestamp"

    with pytest.raises(ValueError, match="timestamp"):
        parse_post_event(data)


def test_cursor_save_and_load(tmp_path) -> None:
    """A saved cursor should survive a simulated collector restart."""

    path = tmp_path / "checkpoints" / "bluesky_cursor.json"

    assert load_cursor(path) is None

    save_cursor(path, 1725911162329308)

    assert load_cursor(path) == 1725911162329308


def test_subscription_url_cursor_overlap() -> None:
    """Reconnection should request a small overlap of past events."""

    url = build_subscription_url(
        base_url="wss://jetstream2.us-east.bsky.network/subscribe",
        collection="app.bsky.feed.post",
        cursor_us=1725911162329308,
        overlap_seconds=5,
    )

    params = parse_qs(urlsplit(url).query)

    assert params["wantedCollections"] == ["app.bsky.feed.post"]
    assert params["cursor"] == [str(1725911162329308 - 5_000_000)]


def test_fresh_subscription_has_no_cursor() -> None:
    """A fresh collector should subscribe to current live events."""

    url = build_subscription_url(
        base_url="wss://jetstream2.us-east.bsky.network/subscribe",
        collection="app.bsky.feed.post",
        cursor_us=None,
        overlap_seconds=5,
    )

    params = parse_qs(urlsplit(url).query)

    assert "cursor" not in params
