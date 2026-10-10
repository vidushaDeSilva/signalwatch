-- S6: Create the source-agnostic Silver tables for SignalWatch.
-- Execute once in the Databricks SQL Editor before running the notebook.

CREATE SCHEMA IF NOT EXISTS signalwatch.silver;

CREATE TABLE IF NOT EXISTS signalwatch.silver.canonical_events (
    event_id STRING,
    source STRING,
    event_type STRING,
    event_time TIMESTAMP,
    actor_id STRING,
    entity_id STRING,
    entity_type STRING,
    content_text STRING,
    language_codes ARRAY<STRING>,
    content_created_at TIMESTAMP,
    source_metadata_json STRING,
    kafka_topic STRING,
    kafka_partition INT,
    kafka_offset BIGINT,
    bronze_batch_id STRING,
    bronze_ingested_at TIMESTAMP,
    silver_ingested_at TIMESTAMP
)
USING DELTA
COMMENT 'Canonical cross-source event representation for SignalWatch';

CREATE TABLE IF NOT EXISTS signalwatch.silver.rejected_events (
    event_id STRING,
    source STRING,
    kafka_topic STRING,
    kafka_partition INT,
    kafka_offset BIGINT,
    rejection_reason STRING,
    raw_json STRING,
    rejected_at TIMESTAMP
)
USING DELTA
COMMENT 'Bronze records that failed Silver canonical normalization';
