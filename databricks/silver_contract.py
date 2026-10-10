"""S7: Validate and normalize legacy Bluesky Jetstream post events.

Pure Python contract shared by local pytest tests and the Databricks Spark UDF.
No Databricks dependency is required to test parser behavior.
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any

CONTRACT_VERSION = "bluesky.jetstream.post.v1"
POST_COLLECTION = "app.bsky.feed.post"
MAX_RAW_JSON_BYTES = 1_048_576
MAX_TIME_US = 253_402_300_800_000_000  # Exclusive: year 10000 UTC.
OPERATIONS = {
    "create": "content_created",
    "update": "content_updated",
    "delete": "content_deleted",
}

# Known optional post extensions are preserved in Bronze, but not mapped into
# generic Silver columns. Unknown keys are *accepted* and recorded as drift.
KNOWN_KEYS = {
    "$": {"did", "kind", "time_us", "commit"},
    "$.commit": {"operation", "collection", "rkey", "rev", "cid", "record"},
    "$.commit.record": {
        "$type", "text", "createdAt", "langs", "facets", "embed",
        "reply", "tags", "labels",
    },
}


def _kind(value: Any) -> str:
    """Represent a JSON value's type without discarding array element types."""
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, str):
        return "string"
    if isinstance(value, int):
        return "integer"
    if isinstance(value, float):
        return "number"
    if isinstance(value, dict):
        return "object"
    if isinstance(value, list):
        types = sorted({_kind(item) for item in value[:100]})
        return "array<" + ",".join(types) + ">"
    return "unknown"


def _observe_shape(payload: dict[str, Any]) -> tuple[str, list[str]]:
    """Fingerprint field *types* at the boundaries governed by this contract.

    The stable hash is a shape identifier, not a source-published version.
    An added field or changed type produces a different shape fingerprint.
    """
    sections: dict[str, Any] = {"$": payload}
    commit = payload.get("commit")
    if isinstance(commit, dict):
        sections["$.commit"] = commit
        record = commit.get("record")
        if isinstance(record, dict):
            sections["$.commit.record"] = record

    shape = {}
    unknown_fields = []
    for path, obj in sections.items():
        shape[path] = {key: _kind(value) for key, value in sorted(obj.items())}
        unknown_fields.extend(
            f"{path}.{key}"
            for key in obj
            if key not in KNOWN_KEYS[path]
        )

    serialized = json.dumps(shape, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
    fingerprint = hashlib.sha256(serialized.encode("utf-8")).hexdigest()
    return fingerprint, sorted(unknown_fields)


def _parse_iso_time_us(value: str) -> int | None:
    """Convert an offset-aware ISO-8601 timestamp to UTC microseconds."""
    try:
        date = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if date.tzinfo is None:
            return None
        utc = date.astimezone(timezone.utc)
        # Avoid floating point epoch conversion on fractional timestamps.
        epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
        difference = utc - epoch
        return ((difference.days * 86400 + difference.seconds) * 1_000_000
                + difference.microseconds)
    except (ValueError, OverflowError):
        return None


def _result(reason: str | None = None) -> dict[str, Any]:
    """Return a stable, Spark-UDF-compatible struct for valid and bad rows."""
    return {
        "rejection_reason": reason,
        "event_id": None,
        "event_type": None,
        "event_time_us": None,
        "actor_id": None,
        "entity_id": None,
        "entity_type": None,
        "content_text": None,
        "language_codes": [],
        "content_created_at_us": None,
        "source_metadata_json": None,
        "schema_version": CONTRACT_VERSION,
        "shape_fingerprint": None,
        "unknown_fields_json": "[]",
    }


def normalize_bluesky_event(raw_json: str | None, source_time_us: int | None) -> dict[str, Any]:
    """Validate one Bronze raw payload and return canonical values or a reason.

    Expected errors are row-level data quality outcomes, not Spark exceptions.
    The *Kafka occurrence identity* is handled separately in the Spark job;
    event_id here identifies the logical upstream commit using repo revision.
    """
    result = _result()
    if not isinstance(raw_json, str) or not raw_json.strip():
        result["rejection_reason"] = "malformed_json"
        return result
    if len(raw_json.encode("utf-8", errors="replace")) > MAX_RAW_JSON_BYTES:
        result["rejection_reason"] = "oversized_json"
        return result

    try:
        payload = json.loads(raw_json)
    except (ValueError, TypeError):
        result["rejection_reason"] = "malformed_json"
        return result
    if not isinstance(payload, dict):
        result["rejection_reason"] = "invalid_root_type"
        return result

    fingerprint, unknown_fields = _observe_shape(payload)
    result["shape_fingerprint"] = fingerprint
    result["unknown_fields_json"] = json.dumps(unknown_fields, separators=(",", ":"))

    def reject(reason: str) -> dict[str, Any]:
        result["rejection_reason"] = reason
        return result

    kind = payload.get("kind")
    if kind != "commit":
        return reject("unsupported_event_kind")

    commit = payload.get("commit")
    if not isinstance(commit, dict):
        return reject("invalid_field_type:commit")

    operation = commit.get("operation")
    if not isinstance(operation, str) or operation not in OPERATIONS:
        return reject("unsupported_operation")

    collection = commit.get("collection")
    if collection != POST_COLLECTION:
        return reject("unsupported_collection")

    actor = payload.get("did")
    if not isinstance(actor, str) or not actor.strip():
        return reject("missing_actor_did")

    rkey = commit.get("rkey")
    if not isinstance(rkey, str) or not rkey.strip():
        return reject("missing_record_key")

    # Revision is required for semantic deduplication. Without it, a delete
    # and re-create could be incorrectly collapsed into an earlier change.
    rev = commit.get("rev")
    if not isinstance(rev, str) or not rev.strip():
        return reject("missing_revision")

    # Validate both timestamps to avoid silently accepting contradictory
    # Bronze metadata and source payloads.
    event_time = payload.get("time_us")
    if (type(event_time) is not int or type(source_time_us) is not int
            or not (0 < event_time < MAX_TIME_US)
            or not (0 < source_time_us < MAX_TIME_US)):
        return reject("missing_or_invalid_event_timestamp")
    if event_time != source_time_us:
        return reject("timestamp_mismatch")

    record = commit.get("record")
    text = None
    langs: list[str] = []
    created_time = None

    if operation in ("create", "update"):
        if not isinstance(record, dict):
            return reject("missing_or_invalid_post_record")

        text = record.get("text")
        if not isinstance(text, str):
            return reject("invalid_field_type:commit.record.text")

        raw_languages = record.get("langs", [])
        if not isinstance(raw_languages, list) or any(
            not isinstance(lang, str) for lang in raw_languages
        ):
            return reject("invalid_field_type:commit.record.langs")
        langs = list(dict.fromkeys(
            lang.strip().lower() for lang in raw_languages if lang.strip()
        ))

        created_at = record.get("createdAt")
        if created_at is not None:
            if not isinstance(created_at, str):
                return reject("invalid_field_type:commit.record.createdAt")
            created_time = _parse_iso_time_us(created_at)
            if created_time is None:
                return reject("invalid_content_created_at")
    elif record is not None and not isinstance(record, dict):
        return reject("invalid_field_type:commit.record")

    # Check the optional metadata we consume or forward.
    if commit.get("cid") is not None and not isinstance(commit["cid"], str):
        return reject("invalid_field_type:commit.cid")

    entity_id = f"at://{actor}/{collection}/{rkey}"
    identity_parts = ["bluesky", actor, collection, rkey, rev, operation]
    identity_bytes = json.dumps(identity_parts, ensure_ascii=True, separators=(",", ":")).encode()
    logical_id = hashlib.sha256(identity_bytes).hexdigest()

    result.update({
        "event_id": f"bluesky:{logical_id}",
        "event_type": OPERATIONS[operation],
        "event_time_us": event_time,
        "actor_id": actor,
        "entity_id": entity_id,
        "entity_type": "post",
        "content_text": text,
        "language_codes": langs,
        "content_created_at_us": created_time,
        "source_metadata_json": json.dumps({
            "jetstream_kind": kind,
            "operation": operation,
            "collection": collection,
            "rkey": rkey,
            "revision": rev,
            "cid": commit.get("cid"),
            "jetstream_time_us": event_time,
            "unknown_fields": unknown_fields,
        }, ensure_ascii=True, separators=(",", ":"), sort_keys=True),
    })
    return result
