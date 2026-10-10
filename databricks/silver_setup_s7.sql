-- S7: Create versioned Silver tables. Existing S6 data is NOT dropped.
-- Execute once in Databricks SQL Editor before running silver_reliability.py.

CREATE SCHEMA IF NOT EXISTS signalwatch.silver;

-- One row per logical Bluesky commit (identity includes repository revision).
CREATE TABLE IF NOT EXISTS signalwatch.silver.canonical_events_v2 (
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
    silver_ingested_at TIMESTAMP,
    schema_version STRING,
    shape_fingerprint STRING,
    occurrence_id STRING,
    payload_sha256 STRING
)
USING DELTA
COMMENT 'S7 canonical Bluesky events, deduplicated by upstream commit revision';

-- Bad source payloads and unsupported kinds are kept for investigation.
-- The Kafka occurrence key, NOT logical ID, uniquely identifies rejections.
CREATE TABLE IF NOT EXISTS signalwatch.silver.rejected_events_v2 (
    occurrence_id STRING,
    source STRING,
    kafka_topic STRING,
    kafka_partition INT,
    kafka_offset BIGINT,
    rejection_reason STRING,
    schema_version STRING,
    shape_fingerprint STRING,
    unknown_fields_json STRING,
    raw_json STRING,
    bronze_batch_id STRING,
    rejected_at TIMESTAMP
)
USING DELTA
COMMENT 'S7 quarantine for malformed, unsupported, or invalid source events';

-- Keep a trace of repeated upstream commits arriving at new Kafka offsets.
CREATE TABLE IF NOT EXISTS signalwatch.silver.duplicate_occurrences (
    occurrence_id STRING,
    event_id STRING,
    source STRING,
    kafka_topic STRING,
    kafka_partition INT,
    kafka_offset BIGINT,
    payload_sha256 STRING,
    duplicate_reason STRING,
    bronze_batch_id STRING,
    detected_at TIMESTAMP
)
USING DELTA
COMMENT 'Kafka occurrences dropped from logical Silver deduplication';

-- Shape fingerprint is stable for observed field names and types.
-- schema_version identifies *our parser contract*, not a source version.
CREATE TABLE IF NOT EXISTS signalwatch.silver.schema_observations (
    shape_fingerprint STRING,
    schema_version STRING,
    unknown_fields_json STRING,
    occurrence_id STRING,
    sample_rejection_reason STRING,
    first_seen_at TIMESTAMP
)
USING DELTA
COMMENT 'Observed Bluesky JSON field-type shapes and parser contract versions';
