"""
Logging configuration for SignalWatch.

The project uses structured JSON logs so local logs can later be consumed by
monitoring or centralized logging systems without changing every application.
"""

import json
import logging
import sys
from datetime import UTC, datetime


class JsonFormatter(logging.Formatter):
    """Format application log records as one JSON object per line."""

    def format(self, record: logging.LogRecord) -> str:
        """Convert a LogRecord into a JSON-formatted string."""

        log_record = {
            "timestamp": datetime.now(UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }

        if record.exc_info:
            log_record["exception"] = self.formatException(record.exc_info)

        return json.dumps(log_record, ensure_ascii=False)


def configure_logging(level: str = "INFO") -> None:
    """Configure the root logger for SignalWatch processes."""

    root_logger = logging.getLogger()

    # Avoid creating duplicate handlers if setup is called more than once.
    root_logger.handlers.clear()

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())

    root_logger.addHandler(handler)
    root_logger.setLevel(level.upper())


def get_logger(name: str) -> logging.Logger:
    """Return a named logger using the project's global configuration."""

    return logging.getLogger(name)
