-- S6: Explore normalized events and verify Silver data quality.
-- Run in the Databricks SQL Editor after the S6 notebook succeeds.

-- 1. All canonical events by type.
SELECT source, event_type, COUNT(*) AS event_count
FROM signalwatch.silver.canonical_events
GROUP BY source, event_type
ORDER BY source, event_type;

-- 2. Latest canonical events.
SELECT event_time, event_type, actor_id, entity_id,
       content_text, language_codes
FROM signalwatch.silver.canonical_events
ORDER BY event_time DESC
LIMIT 20;

-- 3. Language distribution (only source-declared languages).
SELECT language_code, COUNT(*) AS event_count
FROM signalwatch.silver.canonical_events
LATERAL VIEW EXPLODE(language_codes) languages AS language_code
GROUP BY language_code
ORDER BY event_count DESC;

-- 4. Review records that could not be normalized.
SELECT rejection_reason, COUNT(*) AS rejected_count
FROM signalwatch.silver.rejected_events
GROUP BY rejection_reason
ORDER BY rejected_count DESC;

-- 5. Check there are no repeated event identifiers.
SELECT event_id, COUNT(*) AS occurrences
FROM signalwatch.silver.canonical_events
GROUP BY event_id
HAVING COUNT(*) > 1;

-- 6. Compare number of Bronze source events to both Silver outcomes.
SELECT
  (SELECT COUNT(*) FROM signalwatch.bronze.bluesky_events WHERE source='bluesky')
    AS bronze_events,
  (SELECT COUNT(*) FROM signalwatch.silver.canonical_events WHERE source='bluesky')
    AS canonical_events,
  (SELECT COUNT(*) FROM signalwatch.silver.rejected_events WHERE source='bluesky')
    AS rejected_events;

-- 7. Inspect source-specific metadata without polluting canonical columns.
SELECT event_id,
       get_json_object(source_metadata_json, '$.operation') AS original_operation,
       get_json_object(source_metadata_json, '$.cid') AS record_cid
FROM signalwatch.silver.canonical_events
LIMIT 20;
