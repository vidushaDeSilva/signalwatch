"""
Safely clean abandoned landing work and old uploaded archives.

Never delete batches inside ready/. These still require verified
upload by S4. The command previews candidates unless --apply is used.
"""

import argparse
import logging
import shutil
import time
from pathlib import Path

logger = logging.getLogger("signalwatch.landing.cleanup")


def cleanup_landing(
    root: Path,
    staging_minutes: int = 60,
    archived_days: int = 7,
    apply: bool = False,
) -> list[Path]:
    """
    Find or delete expired temporary batches and uploaded archives.

    Only .partial_* directories in staging/ and directories in
    archived/ can be removed. Complete ready batches are protected.
    """

    candidates: list[Path] = []
    now = time.time()

    policies = (
        (
            root / "staging",
            staging_minutes * 60,
            ".partial_",
        ),
        (
            root / "archived",
            archived_days * 86400,
            None,
        ),
    )

    for directory, max_age, required_prefix in policies:
        if not directory.exists():
            continue

        for path in directory.iterdir():
            if not path.is_dir():
                continue

            if required_prefix is not None and not path.name.startswith(required_prefix):
                continue

            if now - path.stat().st_mtime < max_age:
                continue

            candidates.append(path)

            logger.info(
                "cleanup_%s path=%s",
                "delete" if apply else "candidate",
                path,
            )

            if apply:
                shutil.rmtree(path)

    return candidates


def main() -> None:
    """Preview cleanup candidates or explicitly delete them."""

    from signalwatch.logging_config import configure_logging
    from signalwatch.settings import get_settings

    parser = argparse.ArgumentParser(description="Clean safe SignalWatch landing folders")

    parser.add_argument(
        "--apply",
        action="store_true",
        help="Delete instead of preview",
    )

    args = parser.parse_args()

    settings = get_settings()
    configure_logging(settings.log_level)

    items = cleanup_landing(
        Path(settings.landing_dir),
        settings.landing_staging_stale_minutes,
        settings.landing_archived_retention_days,
        apply=args.apply,
    )

    logger.info(
        "cleanup_finished candidates=%s apply=%s",
        len(items),
        args.apply,
    )


if __name__ == "__main__":
    main()
