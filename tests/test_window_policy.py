"""S8: Offline regression tests for UTC windows and lakehouse lateness policy."""

from datetime import datetime, timedelta, timezone

import pytest

from databricks.window_policy import classify_arrival, watermark_frontier, window_bounds


def t(hour, minute, second=0, tz=timezone.utc):
    return datetime(2026, 10, 9, hour, minute, second, tzinfo=tz)


def test_late_at_1008_is_attributed_to_1000_window():
    observed = classify_arrival(t(10, 1), t(10, 8))
    assert observed["window_start"] == t(10, 0)
    assert observed["window_end"] == t(10, 5)
    assert observed["late_after_window_end"]
    assert observed["delay_seconds"] == 420
    assert not observed["more_than_10m_behind_ingestion"]


def test_half_open_boundary():
    assert window_bounds(t(10, 4, 59))[0] == t(10, 0)
    assert window_bounds(t(10, 5))[0] == t(10, 5)


def test_ingestion_at_exact_window_end_is_late():
    assert classify_arrival(t(10, 1), t(10, 5))["late_after_window_end"]


def test_very_late_observation():
    observed = classify_arrival(t(10, 1), t(10, 30))
    assert observed["more_than_10m_behind_ingestion"]


def test_unknown_ingestion_time():
    observed = classify_arrival(t(10, 1), None)
    assert observed["ingestion_time_missing"]
    assert observed["delay_seconds"] is None


def test_future_clock_skew():
    observed = classify_arrival(t(10, 10), t(10, 4))
    assert observed["future_by_more_than_5m"]
    assert not observed["late_after_window_end"]


def test_utc_normalization_of_offset_time():
    plus_two = timezone(timedelta(hours=2))
    assert window_bounds(t(12, 1, tz=plus_two))[0] == t(10, 0)


def test_reject_naive_timestamps():
    with pytest.raises(ValueError, match="explicit UTC offset"):
        window_bounds(datetime(2026, 10, 9, 10, 1))


def test_watermark_tracks_maximum_seen_event_time_not_wall_clock():
    assert watermark_frontier(t(10, 8), timedelta(minutes=10)) == t(9, 58)
    assert watermark_frontier(t(10, 20), timedelta(minutes=10)) == t(10, 10)


def test_negative_watermark_lag_fails():
    with pytest.raises(ValueError):
        watermark_frontier(t(10, 8), timedelta(seconds=-1))


def test_observed_event_time_watermark_eligibility_changes_after_new_max():
    # This illustrates the frontier; Spark's actual stateful behavior is
    # demonstrated separately in the Databricks Streaming lab.
    assert t(10, 1) > watermark_frontier(t(10, 8), timedelta(minutes=10))
    assert t(10, 2) < watermark_frontier(t(10, 20), timedelta(minutes=10))
