"""S7: Offline regression tests for Bluesky payload validation and identity.

Run using `python -m pytest tests/test_silver_contract.py -q` from the S7 bundle.
No Spark, network, Databricks account, or production data is required.
"""

import copy
import json
import sys
from pathlib import Path

# The Databricks workspace module is importable without Spark locally.
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "databricks"))

from silver_contract import CONTRACT_VERSION, normalize_bluesky_event  # noqa: E402

TIME_US = 1_791_540_000_000_000


def source_event(**overrides):
    """Generate a minimally valid legacy Jetstream post event."""
    data = {
        "did": "did:plc:test-author",
        "kind": "commit",
        "time_us": TIME_US,
        "commit": {
            "operation": "create",
            "collection": "app.bsky.feed.post",
            "rkey": "test-rkey",
            "rev": "3ktestrev",
            "cid": "bafytest",
            "record": {
                "text": "Hello from S7",
                "createdAt": "2026-10-09T12:00:00Z",
                "langs": ["EN", "en", "fr"],
            },
        },
    }
    data.update(overrides)
    return data


def evaluate(event, timestamp=TIME_US):
    """Validate serialized source JSON using a matching Bronze timestamp."""
    return normalize_bluesky_event(json.dumps(event), timestamp)


def test_same_event_twice_yields_same_logical_event_id():
    """Different Kafka offsets are irrelevant to the upstream identity."""
    payload = source_event()
    first = evaluate(payload)
    second = evaluate(copy.deepcopy(payload))
    assert first["rejection_reason"] is None
    assert first["event_id"] == second["event_id"]
    assert first["event_id"].startswith("bluesky:")


def test_missing_timestamp_goes_to_quarantine():
    payload = source_event()
    del payload["time_us"]
    assert evaluate(payload)["rejection_reason"] == "missing_or_invalid_event_timestamp"


def test_missing_bronze_timestamp_goes_to_quarantine():
    assert evaluate(source_event(), None)["rejection_reason"] == "missing_or_invalid_event_timestamp"


def test_invalid_json_goes_to_quarantine():
    result = normalize_bluesky_event('{"kind": "commit",', TIME_US)
    assert result["rejection_reason"] == "malformed_json"
    assert result["event_id"] is None


def test_unknown_event_kind_goes_to_quarantine():
    assert evaluate(source_event(kind="identity"))["rejection_reason"] == "unsupported_event_kind"


def test_unknown_operation_goes_to_quarantine():
    payload = source_event()
    payload["commit"]["operation"] = "archive"
    assert evaluate(payload)["rejection_reason"] == "unsupported_operation"


def test_extra_field_accepted_and_schema_fingerprint_changes():
    old = source_event()
    new = copy.deepcopy(old)
    new["commit"]["record"]["new_feature"] = {"foo": 42}
    original = evaluate(old)
    evolved = evaluate(new)
    assert evolved["rejection_reason"] is None
    assert original["shape_fingerprint"] != evolved["shape_fingerprint"]
    assert "$.commit.record.new_feature" in json.loads(evolved["unknown_fields_json"])
    assert evolved["schema_version"] == CONTRACT_VERSION


def test_changed_text_field_type_goes_to_quarantine():
    payload = source_event()
    payload["commit"]["record"]["text"] = {"message": "new schema"}
    changed = evaluate(payload)
    assert changed["rejection_reason"] == "invalid_field_type:commit.record.text"
    assert changed["shape_fingerprint"] != evaluate(source_event())["shape_fingerprint"]


def test_changed_languages_field_type_goes_to_quarantine():
    payload = source_event()
    payload["commit"]["record"]["langs"] = "en"
    assert evaluate(payload)["rejection_reason"] == "invalid_field_type:commit.record.langs"


def test_timestamp_mismatch_goes_to_quarantine():
    assert evaluate(source_event(), TIME_US + 1)["rejection_reason"] == "timestamp_mismatch"


def test_language_normalization_and_content():
    result = evaluate(source_event())
    assert result["language_codes"] == ["en", "fr"]
    assert result["content_text"] == "Hello from S7"
    assert result["entity_type"] == "post"


def test_delete_without_record_is_valid():
    payload = source_event()
    payload["commit"]["operation"] = "delete"
    payload["commit"].pop("record")
    result = evaluate(payload)
    assert result["rejection_reason"] is None
    assert result["event_type"] == "content_deleted"
    assert result["content_text"] is None


def test_new_revision_same_post_is_a_distinct_event():
    one = source_event()
    two = copy.deepcopy(one)
    two["commit"]["rev"] = "3ktestrev_new"
    assert evaluate(one)["event_id"] != evaluate(two)["event_id"]
    assert evaluate(one)["entity_id"] == evaluate(two)["entity_id"]


def test_missing_revision_is_rejected_to_avoid_false_dedup():
    payload = source_event()
    payload["commit"].pop("rev")
    assert evaluate(payload)["rejection_reason"] == "missing_revision"


def test_non_object_json_is_rejected():
    result = normalize_bluesky_event('["hello"]', TIME_US)
    assert result["rejection_reason"] == "invalid_root_type"


def test_content_timestamp_must_have_timezone():
    payload = source_event()
    payload["commit"]["record"]["createdAt"] = "2026-10-09T12:00:00"
    assert evaluate(payload)["rejection_reason"] == "invalid_content_created_at"


def test_empty_text_is_valid_for_media_only_post():
    payload = source_event()
    payload["commit"]["record"]["text"] = ""
    assert evaluate(payload)["rejection_reason"] is None


def test_unchanged_schema_shape_for_new_text_value():
    before = source_event()
    after = source_event()
    after["commit"]["record"]["text"] = "Different content"
    assert evaluate(before)["shape_fingerprint"] == evaluate(after)["shape_fingerprint"]
