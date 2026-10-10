"""S8: Pure-Python UTC window and lateness policy helpers for local tests.

These helpers document the same semantics used by the Spark Gold pipeline.
No Databricks or PySpark dependencies are required.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

WINDOW_MINUTES = 5
OBSERVED_LATE_THRESHOLD = timedelta(minutes=10)
FUTURE_CLOCK_SKEW_THRESHOLD = timedelta(minutes=5)


def _utc(value: datetime) -> datetime:
    """Require a timezone-aware timestamp and convert it to UTC."""
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("Timestamps must have an explicit UTC offset")
    return value.astimezone(timezone.utc)


def window_bounds(event_time: datetime) -> tuple[datetime, datetime]:
    """Return a half-open, UTC-aligned 5-minute event-time window."""
    instant = _utc(event_time)
    start = instant.replace(
        minute=(instant.minute // WINDOW_MINUTES) * WINDOW_MINUTES,
        second=0,
        microsecond=0,
    )
    return start, start + timedelta(minutes=WINDOW_MINUTES)


def classify_arrival(event_time: datetime, bronze_ingested_at: datetime | None) -> dict:
    """Classify observed lakehouse delay without rejecting an event.

    `bronze_ingested_at` is the time the event entered Bronze, *not* the
    WebSocket/Kafka ingress timestamp. Absence means lateness is unknown.
    """
    start, end = window_bounds(event_time)
    if bronze_ingested_at is None:
        return {
            "window_start": start,
            "window_end": end,
            "delay_seconds": None,
            "late_after_window_end": False,
            "more_than_10m_behind_ingestion": False,
            "future_by_more_than_5m": False,
            "ingestion_time_missing": True,
        }
    arrival = _utc(bronze_ingested_at)
    event = _utc(event_time)
    return {
        "window_start": start,
        "window_end": end,
        "delay_seconds": (arrival - event).total_seconds(),
        "late_after_window_end": arrival >= end,
        "more_than_10m_behind_ingestion": arrival - event > OBSERVED_LATE_THRESHOLD,
        "future_by_more_than_5m": event - arrival > FUTURE_CLOCK_SKEW_THRESHOLD,
        "ingestion_time_missing": False,
    }


def watermark_frontier(max_seen_event_time: datetime, allowed_lateness: timedelta) -> datetime:
    """Illustrate the event-time watermark frontier; not a Spark state store."""
    if allowed_lateness < timedelta(0):
        raise ValueError("Allowed lateness must be nonnegative")
    return _utc(max_seen_event_time) - allowed_lateness
