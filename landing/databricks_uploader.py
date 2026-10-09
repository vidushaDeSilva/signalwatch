"""
Upload validated SignalWatch landing batches into Databricks Volumes.

Uses the Databricks SDK Files API with an OAuth CLI profile.
Uploads data first, verifies checksums, publishes the manifest last,
and archives local batches only after remote verification.

Incomplete uploads remain retryable. A local file lock prevents two
uploader processes from running against the same landing directory.
"""

import argparse
import fcntl
import hashlib
import logging
import os
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import (
    NotFound,
    PermissionDenied,
    Unauthenticated,
)

from landing.batch_writer import BatchWriter
from landing.cleanup import cleanup_landing
from signalwatch.logging_config import configure_logging
from signalwatch.settings import Settings, get_settings

logger = logging.getLogger("signalwatch.landing.databricks_uploader")


def local_sha256(path: Path) -> str:
    """Calculate the SHA-256 checksum of a local file."""

    digest = hashlib.sha256()

    with path.open("rb") as stream:
        for chunk in iter(
            lambda: stream.read(1024 * 1024),
            b"",
        ):
            digest.update(chunk)

    return digest.hexdigest()


def validate_volume_path(volume_path: str) -> str:
    """Ensure uploads target an absolute Unity Catalog Volume path."""

    parts = volume_path.strip("/").split("/")

    if len(parts) < 4 or parts[0] != "Volumes" or any(part in {"", ".", ".."} for part in parts):
        raise ValueError("Invalid Volume path. Expected /Volumes/<catalog>/<schema>/<volume>.")

    return "/" + "/".join(parts)


@contextmanager
def uploader_lock(root: Path) -> Iterator[None]:
    """
    Prevent concurrent local uploaders using the same landing folder.

    The lock is held for the lifetime of the uploader process.
    """

    root.mkdir(parents=True, exist_ok=True)
    lock_path = root / ".databricks_uploader.lock"

    with lock_path.open("a+") as lock_file:
        try:
            fcntl.flock(
                lock_file.fileno(),
                fcntl.LOCK_EX | fcntl.LOCK_NB,
            )
        except BlockingIOError as exc:
            raise RuntimeError("Another Databricks uploader is already running.") from exc

        try:
            yield
        finally:
            fcntl.flock(
                lock_file.fileno(),
                fcntl.LOCK_UN,
            )


class DatabricksUploader:
    """Transfer S3 ready batches into a Unity Catalog Volume."""

    def __init__(
        self,
        settings: Settings,
        files_client: Any | None = None,
        dry_run: bool = False,
    ) -> None:
        """Initialize the local writer and remote Files API client."""

        self.settings = settings
        self.dry_run = dry_run

        self.writer = BatchWriter(
            root=Path(settings.landing_dir),
            min_free_mb=settings.landing_min_free_mb,
        )

        self.volume_root = validate_volume_path(settings.signalwatch_volume_path)

        if dry_run:
            self.files = None
        elif files_client is not None:
            # Allows unit tests without real Databricks credentials.
            self.files = files_client
        else:
            workspace = WorkspaceClient(profile=settings.databricks_config_profile)

            self.files = workspace.files

        # Failed batches remain in ready/ and retry later.
        self.failures: dict[str, int] = {}
        self.retry_after: dict[str, float] = {}

    def remote_directory(self, batch_dir: Path) -> str:
        """Return the Volume location assigned to a landing batch."""

        return f"{self.volume_root}/bluesky/{batch_dir.name}"

    def remote_sha256(self, remote_path: str) -> str | None:
        """
        Download a remote file and calculate its SHA-256 checksum.

        Returns None only when the file does not exist. Authentication,
        permission and transport errors are allowed to propagate.
        """

        try:
            response = self.files.download(remote_path)
        except NotFound:
            return None

        if response.contents is None:
            raise RuntimeError(f"Databricks returned no file content: {remote_path}")

        digest = hashlib.sha256()

        with response.contents as remote_file:
            while True:
                chunk = remote_file.read(1024 * 1024)

                if not chunk:
                    break

                digest.update(chunk)

        return digest.hexdigest()

    def verify_remote_batch(
        self,
        remote_dir: str,
        local_data: Path,
        local_manifest: Path,
    ) -> bool:
        """
        Verify both remote files against their corresponding local files.

        Returns False if the manifest has not been uploaded yet.
        Raises ValueError if an existing remote commit is inconsistent.
        """

        remote_manifest = f"{remote_dir}/manifest.json"
        manifest_hash = self.remote_sha256(remote_manifest)

        if manifest_hash is None:
            return False

        if manifest_hash != local_sha256(local_manifest):
            raise ValueError(f"Remote manifest conflicts with local batch: {remote_dir}")

        remote_data = f"{remote_dir}/events.jsonl"
        data_hash = self.remote_sha256(remote_data)

        if data_hash != local_sha256(local_data):
            raise ValueError(f"Remote committed data checksum mismatch: {remote_dir}")

        return True

    def archive_batch(self, batch_dir: Path) -> Path:
        """
        Move a verified local batch into archived/.

        Reset the directory modification time so the S3 cleanup policy
        measures retention from upload confirmation, not batch creation.
        """

        destination = self.writer.archived / batch_dir.name

        if destination.exists():
            raise FileExistsError(f"Archive destination already exists: {destination}")

        os.rename(batch_dir, destination)
        os.utime(destination, None)

        logger.info(
            "batch_archived path=%s",
            destination,
        )

        return destination

    def upload_batch(self, batch_dir: Path) -> None:
        """
        Upload and verify one S3 batch.

        Never delete an unverified local ready batch. An existing
        complete remote batch is verified and archived without upload.
        """

        if batch_dir.is_symlink() or not batch_dir.is_dir():
            raise ValueError(f"Expected a regular batch directory: {batch_dir}")

        # Recheck integrity because local files may have been changed
        # or corrupted since S3 originally published the batch.
        manifest = self.writer.validate_batch(batch_dir)

        if manifest["batch_id"] != batch_dir.name:
            raise ValueError("Batch directory and manifest ID differ.")

        local_data = batch_dir / "events.jsonl"
        local_manifest = batch_dir / "manifest.json"

        remote_dir = self.remote_directory(batch_dir)

        logger.info(
            "upload_start batch=%s records=%s remote=%s",
            manifest["batch_id"],
            manifest["record_count"],
            remote_dir,
        )

        if self.dry_run:
            logger.info(
                "dry_run_validated batch=%s remote=%s",
                batch_dir.name,
                remote_dir,
            )
            return

        # If the previous run uploaded everything but crashed before
        # local archiving, verification alone is sufficient.
        if self.verify_remote_batch(
            remote_dir,
            local_data,
            local_manifest,
        ):
            logger.info(
                "remote_batch_already_verified batch=%s",
                batch_dir.name,
            )
            self.archive_batch(batch_dir)
            return

        # The directory API creates any missing parent directories.
        self.files.create_directory(remote_dir)

        remote_data = f"{remote_dir}/events.jsonl"
        remote_manifest = f"{remote_dir}/manifest.json"

        # A previous interrupted upload may have left an incomplete
        # data file. Overwriting is safe before the manifest is present.
        self.files.upload_from(
            remote_data,
            str(local_data),
            overwrite=True,
            use_parallel=False,
        )

        if self.remote_sha256(remote_data) != manifest["sha256"]:
            raise ValueError("Uploaded data failed SHA-256 verification.")

        # The manifest is our final remote completion marker.
        # Never silently overwrite an existing manifest.
        self.files.upload_from(
            remote_manifest,
            str(local_manifest),
            overwrite=False,
            use_parallel=False,
        )

        if not self.verify_remote_batch(
            remote_dir,
            local_data,
            local_manifest,
        ):
            raise RuntimeError("Remote batch is missing its completion manifest.")

        logger.info(
            "upload_verified batch=%s bytes=%s",
            batch_dir.name,
            manifest["size_bytes"],
        )

        # Keep the local copy for the configured archive retention.
        self.archive_batch(batch_dir)

    def run_cycle(self) -> tuple[int, int]:
        """
        Attempt a bounded number of ready batches.

        Failed batches stay in ready/ and are retried with exponential
        backoff. Other batches can continue uploading independently.
        """

        candidates = sorted(
            directory
            for directory in self.writer.ready.iterdir()
            if directory.is_dir() and not directory.is_symlink()
        )

        attempted = 0
        failed = 0

        for batch_dir in candidates:
            if attempted >= self.settings.databricks_upload_max_batches_per_cycle:
                break

            if time.monotonic() < self.retry_after.get(batch_dir.name, 0):
                continue

            attempted += 1

            try:
                self.upload_batch(batch_dir)

                self.failures.pop(batch_dir.name, None)
                self.retry_after.pop(batch_dir.name, None)

            except (Unauthenticated, PermissionDenied):
                # Credentials or access permissions need operator action.
                # Retrying continuously cannot fix these problems.
                logger.exception("Databricks authentication or permission failed.")
                raise

            except Exception:
                failed += 1

                count = self.failures.get(batch_dir.name, 0) + 1
                self.failures[batch_dir.name] = count

                delay = min(300, 5 * (2 ** min(count - 1, 6)))

                self.retry_after[batch_dir.name] = time.monotonic() + delay

                logger.exception(
                    "batch_upload_failed batch=%s attempt=%s retry_after_seconds=%s",
                    batch_dir.name,
                    count,
                    delay,
                )

        return attempted, failed

    def run(self, once: bool = False) -> int:
        """
        Run one upload cycle or continuously watch for ready batches.

        Local lock ownership lasts until the process exits.
        Returns a nonzero status if a one-time pass has failures.
        """

        logger.info(
            "uploader_started volume=%s landing_dir=%s",
            self.volume_root,
            self.writer.root,
        )

        with uploader_lock(self.writer.root):
            while True:
                attempted, failed = self.run_cycle()

                logger.info(
                    "upload_cycle_completed attempted=%s failed=%s",
                    attempted,
                    failed,
                )

                if once:
                    return 1 if failed else 0

                # Cleanup only staging and previously verified archives.
                # The S3 cleanup utility deliberately protects ready/.
                cleanup_landing(
                    root=self.writer.root,
                    staging_minutes=(self.settings.landing_staging_stale_minutes),
                    archived_days=(self.settings.landing_archived_retention_days),
                    apply=True,
                )

                time.sleep(self.settings.databricks_upload_poll_seconds)


def main() -> None:
    """Parse command-line options and start the S4 uploader."""

    parser = argparse.ArgumentParser(description="Upload SignalWatch landing batches to Databricks")

    parser.add_argument(
        "--once",
        action="store_true",
        help="Process one bounded upload cycle and exit.",
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate and preview without uploading or archiving.",
    )

    args = parser.parse_args()

    settings = get_settings()
    configure_logging(settings.log_level)

    try:
        uploader = DatabricksUploader(
            settings=settings,
            dry_run=args.dry_run,
        )

        raise SystemExit(uploader.run(once=args.once))

    except KeyboardInterrupt:
        logger.info("Uploader stopped by user.")

    except Exception:
        logger.exception("Databricks uploader stopped with error.")
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
