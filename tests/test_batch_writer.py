"""
Test landing batch publication, integrity, replay and cleanup.

Tests use temporary directories and synthetic Kafka records.
No running Kafka broker or Bluesky connection is required.
"""

import json
import os
import time

import pytest

from landing.batch_writer import BatchWriter
from landing.cleanup import cleanup_landing


def rows(
    partition: int = 1,
    start: int = 10,
    count: int = 3,
) -> list[dict]:
    """Create representative records for one Kafka partition."""

    return [
        {
            "source": "bluesky",
            "kafka_topic": "signalwatch.bluesky.posts.v1",
            "kafka_partition": partition,
            "kafka_offset": start + i,
            "kafka_key": f"at://post/{i}",
            "source_time_us": 1700000000000000 + i,
            "landed_at": "2026-10-09T00:00:00+00:00",
            "raw_json": json.dumps(
                {
                    "kind": "commit",
                    "time_us": 1700000000000000 + i,
                }
            ),
        }
        for i in range(count)
    ]


def test_writer_publishes_valid_directory(tmp_path) -> None:
    """Published batches contain valid data and metadata."""

    writer = BatchWriter(
        tmp_path / "landing",
        min_free_mb=0,
    )

    saved = writer.write_batch(rows())
    manifest = writer.validate_batch(saved.path)

    assert saved.path.is_dir()
    assert manifest["record_count"] == 3
    assert manifest["next_offset"] == 13
    assert list(writer.staging.iterdir()) == []

    assert len((saved.path / "events.jsonl").read_text().splitlines()) == 3


def test_replayed_range_does_not_overwrite_ready(tmp_path) -> None:
    """Identical Kafka ranges reuse an existing valid batch."""

    writer = BatchWriter(
        tmp_path / "landing",
        min_free_mb=0,
    )

    first = writer.write_batch(rows())
    second = writer.write_batch(rows())

    assert first.path == second.path
    assert len(list(writer.ready.iterdir())) == 1


def test_corrupted_ready_batch_is_detected(tmp_path) -> None:
    """Corrupted files fail validation."""

    writer = BatchWriter(
        tmp_path / "landing",
        min_free_mb=0,
    )

    saved = writer.write_batch(rows())

    (saved.path / "events.jsonl").write_text("{}\n")

    with pytest.raises((ValueError, KeyError)):
        writer.validate_batch(saved.path)


def test_mixed_partitions_are_rejected(tmp_path) -> None:
    """One file batch cannot mix different Kafka partitions."""

    writer = BatchWriter(
        tmp_path / "landing",
        min_free_mb=0,
    )

    with pytest.raises(
        ValueError,
        match="one Kafka partition",
    ):
        writer.write_batch(rows(partition=0) + rows(partition=1))


def test_cleanup_never_deletes_ready(tmp_path) -> None:
    """Automatic cleanup protects ready batches from deletion."""

    writer = BatchWriter(
        tmp_path / "landing",
        min_free_mb=0,
    )

    published = writer.write_batch(rows())

    partial = writer.staging / ".partial_crash"
    partial.mkdir()

    old = time.time() - 8 * 86400
    os.utime(partial, (old, old))

    candidates = cleanup_landing(
        writer.root,
        staging_minutes=60,
        apply=True,
    )

    assert partial in candidates
    assert not partial.exists()
    assert published.path.exists()
