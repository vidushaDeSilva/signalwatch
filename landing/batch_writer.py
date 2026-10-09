"""
Write Kafka events into validated, recoverable local landing batches.

Each batch contains a JSONL data file and a manifest. The complete
directory moves from staging to ready before Kafka offsets may be
committed by the landing consumer.
"""

import hashlib
import json
import logging
import os
import shutil
import tempfile
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

logger = logging.getLogger("signalwatch.landing.writer")


@dataclass(frozen=True)
class ReadyBatch:
    """Details of a complete, validated landing batch."""

    path: Path
    count: int
    partition: int
    next_offset: int


class BatchWriter:
    """Persist one Kafka partition's records as a self-contained batch."""

    def __init__(
        self,
        root: Path,
        min_free_mb: int = 256,
    ) -> None:
        """Create landing directories and configure disk protection."""

        self.root = root
        self.staging = root / "staging"
        self.ready = root / "ready"
        self.archived = root / "archived"
        self.min_free_mb = min_free_mb

        for directory in (
            self.staging,
            self.ready,
            self.archived,
        ):
            directory.mkdir(parents=True, exist_ok=True)

    def check_disk_space(self) -> None:
        """Stop writing when the disk reserve is exhausted."""

        free_bytes = shutil.disk_usage(self.root).free

        if free_bytes < self.min_free_mb * 1024 * 1024:
            raise OSError(f"Landing disk space too low: {free_bytes // (1024 * 1024)} MB free")

    @staticmethod
    def _offset_digest(records: list[dict]) -> str:
        """Fingerprint all Kafka offsets, including any gaps."""

        offsets = ",".join(str(row["kafka_offset"]) for row in records)

        return hashlib.sha256(offsets.encode("ascii")).hexdigest()

    @staticmethod
    def validate_batch(batch_dir: Path) -> dict:
        """
        Verify a batch's manifest, record count, checksum and offsets.

        Raises an exception if the data or metadata is inconsistent.
        """

        manifest_path = batch_dir / "manifest.json"
        data_path = batch_dir / "events.jsonl"

        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

        checksum = hashlib.sha256()
        offsets = []
        rows = 0

        with data_path.open("rb") as stream:
            for line in stream:
                checksum.update(line)

                row = json.loads(line)

                try:
                    topic = row["kafka_topic"]
                    partition = row["kafka_partition"]
                    offset = row["kafka_offset"]

                except (KeyError, TypeError) as exc:
                    raise ValueError(
                        "Landing batch contains a record with missing or invalid metadata"
                    ) from exc

                if topic != manifest["kafka_topic"]:
                    raise ValueError("Topic mismatch in landing batch")

                if partition != manifest["kafka_partition"]:
                    raise ValueError("Partition mismatch in landing batch")

                if type(offset) is not int or offset < 0:
                    raise ValueError("Invalid Kafka offset in landing batch")

                offsets.append(offset)

                rows += 1

        if not offsets or offsets != sorted(set(offsets)):
            raise ValueError("Invalid or unordered Kafka offsets in batch")

        if rows != manifest["record_count"]:
            raise ValueError("Record count mismatch in landing batch")

        if offsets[0] != manifest["first_offset"]:
            raise ValueError("First offset mismatch in landing batch")

        if offsets[-1] != manifest["last_offset"]:
            raise ValueError("Last offset mismatch in landing batch")

        if checksum.hexdigest() != manifest["sha256"]:
            raise ValueError("Checksum mismatch in landing batch")

        if data_path.stat().st_size != manifest["size_bytes"]:
            raise ValueError("File size mismatch in landing batch")

        offset_hash = hashlib.sha256(
            ",".join(str(offset) for offset in offsets).encode("ascii")
        ).hexdigest()

        if offset_hash != manifest["offsets_sha256"]:
            raise ValueError("Offset sequence mismatch in landing batch")

        return manifest

    def write_batch(self, records: list[dict]) -> ReadyBatch:
        """
        Write, validate, and publish one Kafka partition batch.

        An existing valid batch with the same Kafka offset sequence
        is reused rather than overwritten.
        """

        if not records:
            raise ValueError("Cannot write an empty batch")

        topic = records[0]["kafka_topic"]
        partition = records[0]["kafka_partition"]

        offsets = [row["kafka_offset"] for row in records]

        if any(
            row["kafka_topic"] != topic or row["kafka_partition"] != partition for row in records
        ):
            raise ValueError("A batch must contain only one Kafka partition")

        if offsets != sorted(set(offsets)):
            raise ValueError("Kafka offsets must be unique and increasing")

        self.check_disk_space()

        first = offsets[0]
        last = offsets[-1]

        # Kafka offset ranges are reproducible batch identifiers.
        batch_id = f"bluesky_p{partition:03d}_o{first:020d}-{last:020d}"

        destination = self.ready / batch_id
        expected_hash = self._offset_digest(records)

        # A crash after publishing but before committing may replay
        # the same Kafka records.
        if destination.exists():
            existing = self.validate_batch(destination)

            if (
                existing["kafka_topic"] != topic
                or existing["record_count"] != len(records)
                or existing["offsets_sha256"] != expected_hash
            ):
                raise ValueError(f"Conflicting existing landing batch: {destination}")

            return ReadyBatch(
                destination,
                len(records),
                partition,
                last + 1,
            )

        # A temporary directory keeps incomplete work out of ready/.
        stage = Path(
            tempfile.mkdtemp(
                prefix=".partial_",
                dir=self.staging,
            )
        )

        try:
            data_file = stage / "events.jsonl"
            checksum = hashlib.sha256()
            size_bytes = 0

            with data_file.open("wb") as output:
                for row in records:
                    line = (
                        json.dumps(
                            row,
                            ensure_ascii=False,
                            separators=(",", ":"),
                        )
                        + "\n"
                    ).encode("utf-8")

                    output.write(line)
                    checksum.update(line)
                    size_bytes += len(line)

                output.flush()
                os.fsync(output.fileno())

            source_times = [
                row["source_time_us"] for row in records if type(row.get("source_time_us")) is int
            ]

            manifest = {
                "format_version": 1,
                "batch_id": batch_id,
                "source": "bluesky",
                "status": "ready",
                "created_at": datetime.now(UTC).isoformat(),
                "kafka_topic": topic,
                "kafka_partition": partition,
                "first_offset": first,
                "last_offset": last,
                "next_offset": last + 1,
                "record_count": len(records),
                "data_file": "events.jsonl",
                "size_bytes": size_bytes,
                "sha256": checksum.hexdigest(),
                "offsets_sha256": expected_hash,
                "min_source_time_us": (min(source_times) if source_times else None),
                "max_source_time_us": (max(source_times) if source_times else None),
            }

            manifest_path = stage / "manifest.json"

            with manifest_path.open(
                "w",
                encoding="utf-8",
            ) as output:
                json.dump(manifest, output, indent=2)
                output.flush()
                os.fsync(output.fileno())

            # Check the whole batch before making it visible.
            self.validate_batch(stage)

            # Staging and ready are on the same filesystem.
            # Renaming the directory publishes both files together.
            os.rename(stage, destination)

            logger.info(
                "batch_ready path=%s records=%s",
                destination,
                len(records),
            )

            return ReadyBatch(
                destination,
                len(records),
                partition,
                last + 1,
            )

        finally:
            # Interrupted or failed writes must not remain active.
            if stage.exists():
                shutil.rmtree(stage)
