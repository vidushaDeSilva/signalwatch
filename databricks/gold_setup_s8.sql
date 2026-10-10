-- S8: Create the event-time Gold schema and a Delta table for 5-minute posts.
-- Run this once in Databricks SQL Editor. Nothing in Bronze/Silver is altered.

CREATE SCHEMA IF NOT EXISTS signalwatch.gold;

CREATE TABLE IF NOT EXISTS signalwatch.gold.bluesky_posts_5m (
    source STRING,
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    post_count BIGINT,
    late_post_count BIGINT,
    very_late_post_count BIGINT,
    out_of_order_post_count BIGINT,
    future_dated_post_count BIGINT,
    unknown_ingestion_time_count BIGINT,
    max_ingestion_delay_seconds DOUBLE,
    last_event_time TIMESTAMP,
    latest_bronze_ingested_at TIMESTAMP,
    aggregate_updated_at TIMESTAMP
)
USING DELTA
COMMENT 'S8 recomputed 5-minute event-time counts from canonical Bluesky post creates; corrected for arbitrarily late Bronze arrivals';

-- Required checkpoint storage for the optional Structured Streaming lab.
-- This is NOT a Spark DataFrame checkpoint and is not used by production Gold.
CREATE VOLUME IF NOT EXISTS signalwatch.gold.s8_stream_checkpoints
COMMENT 'Isolated S8 watermark lab streaming checkpoint storage';
