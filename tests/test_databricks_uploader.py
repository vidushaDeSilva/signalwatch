"""
Test Databricks batch uploading without making cloud API calls.

Uses an in-memory Files API to test completed uploads, retries,
lost responses, archive behavior and checksum verification.
"""

import io
import shutil
from pathlib import Path
from types import SimpleNamespace

import pytest
from databricks.sdk.errors import NotFound

from landing.batch_writer import BatchWriter
from landing.databricks_uploader import (
    DatabricksUploader,
    local_sha256,
    validate_volume_path,
)


class FakeFilesAPI:
    """Simulate the Files API using an in-memory dictionary."""

    def __init__(self) -> None:
        """Initialize simulated remote files and upload history."""

        self.remote_files: dict[str, bytes] = {}
        self.operations: list[str] = []
        self.fail_manifest_once = False

    def create_directory(self, path: str) -> None:
        """Accept directory creation and record the operation."""

        self.operations.append(f"mkdir:{path}")

    def upload_from(
        self,
        file_path: str,
        source_path: str,
        overwrite: bool = True,
        use_parallel: bool = False,
    ) -> None:
        """Copy a local file into simulated remote storage."""

        del use_parallel

        if not overwrite and file_path in self.remote_files:
            raise FileExistsError("Remote file already exists")

        self.remote_files[file_path] = Path(source_path).read_bytes()

        self.operations.append(f"upload:{file_path}")

        if self.fail_manifest_once and file_path.endswith("/manifest.json"):
            self.fail_manifest_once = False

            # Simulate a successful remote upload followed by
            # a lost HTTP response before the client learns of it.
            raise ConnectionError("Lost upload acknowledgement")

    def download(self, file_path: str) -> SimpleNamespace:
        """Return remote bytes or raise NotFound."""

        if file_path not in self.remote_files:
            raise NotFound("Remote file not found")

        return SimpleNamespace(contents=io.BytesIO(self.remote_files[file_path]))


def sample_rows() -> list[dict]:
    """Generate representative S3 landed Kafka records."""

    rows = []

    for offset in range(10, 13):
        rows.append(
            {
                "source": "bluesky",
                "kafka_topic": "signalwatch.bluesky.posts.v1",
                "kafka_partition": 0,
                "kafka_offset": offset,
                "kafka_key": f"at://example/post/{offset}",
                "source_time_us": 1791540000000000 + offset,
                "landed_at": "2026-10-09T10:00:00+00:00",
                "raw_json": '{"kind":"commit"}',
            }
        )

    return rows


def setup_uploader(
    tmp_path: Path,
    files_api: FakeFilesAPI,
) -> tuple[DatabricksUploader, Path]:
    """Create an S3 batch and S4 uploader with test settings."""

    root = tmp_path / "landing"

    writer = BatchWriter(
        root=root,
        min_free_mb=0,
    )

    batch = writer.write_batch(sample_rows())

    settings = SimpleNamespace(
        landing_dir=str(root),
        landing_min_free_mb=0,
        signalwatch_volume_path="/Volumes/signalwatch/bronze/landing",
        databricks_upload_max_batches_per_cycle=5,
    )

    uploader = DatabricksUploader(
        settings=settings,
        files_client=files_api,
    )

    return uploader, batch.path


def test_upload_and_archive(tmp_path) -> None:
    """A completed remote upload should archive the local batch."""

    files_api = FakeFilesAPI()
    uploader, batch_dir = setup_uploader(tmp_path, files_api)

    remote_dir = uploader.remote_directory(batch_dir)

    uploader.upload_batch(batch_dir)

    assert not batch_dir.exists()
    assert (uploader.writer.archived / batch_dir.name).exists()

    assert f"{remote_dir}/events.jsonl" in files_api.remote_files
    assert f"{remote_dir}/manifest.json" in files_api.remote_files

    uploads = [operation for operation in files_api.operations if operation.startswith("upload:")]

    # Data must be uploaded before the completion manifest.
    assert uploads[0].endswith("/events.jsonl")
    assert uploads[1].endswith("/manifest.json")


def test_lost_manifest_response_is_recoverable(tmp_path) -> None:
    """An interrupted response must not cause permanent data loss."""

    files_api = FakeFilesAPI()
    uploader, batch_dir = setup_uploader(tmp_path, files_api)

    files_api.fail_manifest_once = True

    with pytest.raises(ConnectionError):
        uploader.upload_batch(batch_dir)

    assert batch_dir.exists()

    # Both remote files were actually uploaded.
    # The second attempt verifies instead of overwriting them.
    uploader.upload_batch(batch_dir)

    assert not batch_dir.exists()
    assert (uploader.writer.archived / batch_dir.name).exists()


def test_corrupted_local_batch_is_not_uploaded(tmp_path) -> None:
    """
    Verify that a modified batch fails integrity validation.

    Changes the content of a valid JSONL record without changing
    its required structure, causing a checksum mismatch.
    """

    files_api = FakeFilesAPI()
    uploader, batch_dir = setup_uploader(tmp_path, files_api)

    data_file = batch_dir / "events.jsonl"

    # Preserve valid JSON and required fields, but modify the data.
    original_content = data_file.read_bytes()

    corrupted_content = original_content.replace(
        b'"source":"bluesky"',
        b'"source":"changed"',
        1,
    )

    assert corrupted_content != original_content

    data_file.write_bytes(corrupted_content)

    # The manifest checksum should no longer match the data.
    with pytest.raises(ValueError, match="Checksum mismatch"):
        uploader.upload_batch(batch_dir)

    # Corrupted batches must not be uploaded or archived.
    assert batch_dir.exists()
    assert files_api.remote_files == {}


def test_remote_committed_corruption_is_detected(tmp_path) -> None:
    """Never overwrite a remote batch with a published manifest."""

    files_api = FakeFilesAPI()
    uploader, batch_dir = setup_uploader(tmp_path, files_api)

    remote_dir = uploader.remote_directory(batch_dir)

    uploader.upload_batch(batch_dir)

    # Simulate the original batch becoming available locally again.
    archived = uploader.writer.archived / batch_dir.name
    shutil.move(str(archived), str(batch_dir))

    files_api.remote_files[f"{remote_dir}/events.jsonl"] = b"corrupted remote content"

    with pytest.raises(ValueError, match="checksum"):
        uploader.upload_batch(batch_dir)

    assert batch_dir.exists()


def test_volume_path_validation() -> None:
    """Reject invalid destination roots."""

    assert (
        validate_volume_path("/Volumes/signalwatch/bronze/landing")
        == "/Volumes/signalwatch/bronze/landing"
    )

    with pytest.raises(ValueError):
        validate_volume_path("/tmp/signalwatch")


def test_local_checksum(tmp_path) -> None:
    """Checksum helper must hash file bytes correctly."""

    file_path = tmp_path / "test.txt"
    file_path.write_bytes(b"hello")

    assert local_sha256(file_path) == (
        "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
    )
