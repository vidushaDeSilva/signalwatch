-- S8: Inspect and reconcile time-windowed SignalWatch Gold counts.

-- 1. Event-time windows are UTC and half-open: [window_start, window_end).
SELECT window_start, window_end, post_count, late_post_count,
       very_late_post_count, out_of_order_post_count,
       future_dated_post_count, unknown_ingestion_time_count,
       aggregate_updated_at
FROM signalwatch.gold.bluesky_posts_5m
ORDER BY window_start DESC
LIMIT 50;

-- 2. Gold must count exactly one row per canonical post-create event.
SELECT
    (SELECT COUNT(*) FROM signalwatch.silver.canonical_events_v2
     WHERE source = 'bluesky' AND event_type = 'content_created'
       AND entity_type = 'post') AS silver_post_creates,
    (SELECT COALESCE(SUM(post_count), 0)
     FROM signalwatch.gold.bluesky_posts_5m
     WHERE source = 'bluesky') AS gold_post_creates;

-- 3. Investigate observed warehouse-ingestion lateness and clock anomalies.
SELECT
    SUM(late_post_count) AS post_arrivals_after_window_end,
    SUM(very_late_post_count) AS more_than_10m_after_event_time,
    SUM(out_of_order_post_count) AS out_of_order_in_kafka_partition,
    SUM(future_dated_post_count) AS events_more_than_5m_ahead_of_ingestion,
    SUM(unknown_ingestion_time_count) AS unknown_ingestion_time
FROM signalwatch.gold.bluesky_posts_5m;

-- 4. Inspect the question's exact event-time window (UTC) in your Gold table.
-- The sample date here is for a synthetic test; it will be absent from
-- production unless real data occurred in that time period.
SELECT * FROM signalwatch.gold.bluesky_posts_5m
WHERE window_start = TIMESTAMP '2026-10-09 10:00:00';

-- 5. Duplicate Gold window keys should never appear.
SELECT source, window_start, window_end, COUNT(*) AS occurrences
FROM signalwatch.gold.bluesky_posts_5m
GROUP BY source, window_start, window_end
HAVING COUNT(*) > 1;

-- 6. Confirm Delta as in S5/S7 troubleshooting.
DESCRIBE DETAIL signalwatch.gold.bluesky_posts_5m;
