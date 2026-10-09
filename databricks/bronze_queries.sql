
-- S5: Verify Bronze ingestion and explore real Bluesky events.
-- Run these queries individually in Databricks SQL Editor.


-- 1. Count historical events currently available in Bronze.

SELECT COUNT(*) AS total_events
FROM signalwatch.bronze.bluesky_events;


-- 2. Inspect recent events while preserving the raw payload.

SELECT
    kafka_partition,
    kafka_offset,
    kafka_key,
    timestamp_micros(source_time_us) AS source_time,
    raw_json
FROM signalwatch.bronze.bluesky_events
ORDER BY source_time_us DESC
LIMIT 20;


-- 3. Extract post text directly from the retained raw JSON.
-- This is a query-time projection, not a Bronze transformation.

SELECT
    kafka_key,
    timestamp_micros(source_time_us) AS source_time,
    get_json_object(
        raw_json,
        '$.commit.record.text'
    ) AS post_text
FROM signalwatch.bronze.bluesky_events
WHERE get_json_object(
    raw_json,
    '$.commit.record.text'
) IS NOT NULL
ORDER BY source_time_us DESC
LIMIT 30;


-- 4. Count events by Kafka partition.

SELECT
    kafka_partition,
    COUNT(*) AS event_count,
    MIN(kafka_offset) AS first_offset,
    MAX(kafka_offset) AS last_offset
FROM signalwatch.bronze.bluesky_events
GROUP BY kafka_partition
ORDER BY kafka_partition;


-- 5. Inspect completed ingestion batches.

SELECT
    source_batch_id,
    record_count,
    processed_at,
    landing_file_path
FROM signalwatch.bronze.bluesky_ingested_batches
ORDER BY processed_at DESC
LIMIT 20;


-- 6. Check that no Kafka message identity was duplicated.

SELECT
    kafka_topic,
    kafka_partition,
    kafka_offset,
    COUNT(*) AS occurrences
FROM signalwatch.bronze.bluesky_events
GROUP BY
    kafka_topic,
    kafka_partition,
    kafka_offset
HAVING COUNT(*) > 1;


-- 7. See the distribution of records over time.

SELECT
    DATE(
        timestamp_micros(source_time_us)
    ) AS event_date,
    COUNT(*) AS event_count
FROM signalwatch.bronze.bluesky_events
GROUP BY event_date
ORDER BY event_date;


-- 8. Inspect Delta table history.

DESCRIBE HISTORY signalwatch.bronze.bluesky_events;
