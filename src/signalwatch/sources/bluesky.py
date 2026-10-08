"""
Utilities for Bluesky Jetstream events and cursor management.

Parses public post events, builds subscription URLs, and saves the
last processed Jetstream timestamp so the collector can resume.
No Kafka or database operations are performed here.
"""

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit


@dataclass(frozen=True)
class BlueskyPostEvent:
    """Basic metadata extracted from a Bluesky post operation."""

    did: str
    uri: str
    operation: str
    time_us: int
    created_at: str | None
    language: str | None
    text: str | None


def parse_post_event(data: dict[str, Any]) -> BlueskyPostEvent | None:
    """
    Parse a Jetstream post commit.

    Returns None for non-post events, such as account updates
    or commits involving other collections.

    A delete operation normally does not include the post record.
    """

    if data.get("kind") != "commit":
        return None

    commit = data.get("commit")

    if not isinstance(commit, dict):
        raise ValueError("Commit event has no valid commit object.")

    if commit.get("collection") != "app.bsky.feed.post":
        return None

    did = data.get("did")
    time_us = data.get("time_us")
    rkey = commit.get("rkey")
    operation = commit.get("operation")

    if not isinstance(did, str) or not did:
        raise ValueError("Missing Bluesky DID.")

    if type(time_us) is not int or time_us <= 0:
        raise ValueError("Invalid Jetstream timestamp.")

    if not isinstance(rkey, str) or not rkey:
        raise ValueError("Missing post record key.")

    if operation not in {"create", "update", "delete"}:
        raise ValueError(f"Unsupported post operation: {operation}")

    record = commit.get("record")

    if operation != "delete" and not isinstance(record, dict):
        raise ValueError("Create/update event has no post record.")

    if not isinstance(record, dict):
        record = {}

    languages = record.get("langs")
    language = None

    if isinstance(languages, list) and languages:
        if isinstance(languages[0], str):
            language = languages[0]

    text = record.get("text")
    created_at = record.get("createdAt")

    return BlueskyPostEvent(
        did=did,
        uri=f"at://{did}/app.bsky.feed.post/{rkey}",
        operation=operation,
        time_us=time_us,
        created_at=created_at if isinstance(created_at, str) else None,
        language=language,
        text=text if isinstance(text, str) else None,
    )


def build_subscription_url(
    base_url: str,
    collection: str,
    cursor_us: int | None,
    overlap_seconds: int,
) -> str:
    """Build a Jetstream v1 URL with collection filtering and replay."""

    parts = urlsplit(base_url)

    if parts.scheme not in {"ws", "wss"}:
        raise ValueError("Jetstream URL must use ws:// or wss://.")

    # Preserve unrelated query parameters, but replace our filters.
    params = [
        (key, value)
        for key, value in parse_qsl(parts.query)
        if key not in {"wantedCollections", "cursor"}
    ]

    params.append(("wantedCollections", collection))

    if cursor_us is not None:
        rewind_us = overlap_seconds * 1_000_000
        params.append(("cursor", str(max(0, cursor_us - rewind_us))))

    return urlunsplit(
        (
            parts.scheme,
            parts.netloc,
            parts.path,
            urlencode(params),
            parts.fragment,
        )
    )


def load_cursor(path: Path) -> int | None:
    """
    Load the last saved Jetstream timestamp.

    If the file does not exist, collection starts at the live tip.
    An invalid existing checkpoint raises an error rather than
    silently losing our saved position.
    """

    if not path.exists():
        return None

    data = json.loads(path.read_text(encoding="utf-8"))
    cursor = data.get("time_us")

    if type(cursor) is not int or cursor <= 0:
        raise ValueError(f"Invalid cursor checkpoint: {path}")

    return cursor


def save_cursor(path: Path, cursor_us: int) -> None:
    """
    Save the current Jetstream timestamp locally.

    Write to a temporary file and then replace the checkpoint so
    interruptions do not normally leave a partially written file.
    """

    path.parent.mkdir(parents=True, exist_ok=True)

    payload = {
        "time_us": cursor_us,
        "saved_at": datetime.now(UTC).isoformat(),
    }

    temporary_path = path.with_suffix(path.suffix + ".tmp")

    temporary_path.write_text(
        json.dumps(payload, indent=2),
        encoding="utf-8",
    )

    temporary_path.replace(path)
