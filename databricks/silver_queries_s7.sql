-- S7: Validation SQL. All statements are read-only.

-- 1. Reconcile every Kafka Bronze occurrence: one canonical, rejected,
--    or recorded as a duplicate, assuming unique Kafka coordinates in Bronze.
WITH counts AS (
  SELECT
    (SELECT COUNT(*) FROM signalwatch.bronze.bluesky_events WHERE source = 'bluesky') AS bronze,
    (SELECT COUNT(*) FROM signalwatch.silver.canonical_events_v2) AS canonical,
    (SELECT COUNT(*) FROM signalwatch.silver.rejected_events_v2) AS quarantined,
    (SELECT COUNT(*) FROM signalwatch.silver.duplicate_occurrences) AS duplicates
)
SELECT *, bronze - (canonical + quarantined + duplicates) AS unaccounted
FROM counts;

-- 2. Should return zero rows: only one row per logical source event.
SELECT event_id, COUNT(*) AS occurrences
FROM signalwatch.silver.canonical_events_v2
GROUP BY event_id
HAVING COUNT(*) > 1;

-- 3. Quarantine reasons and number of affected source occurrences.
SELECT rejection_reason, COUNT(*) AS events
FROM signalwatch.silver.rejected_events_v2
GROUP BY rejection_reason
ORDER BY events DESC;

-- 4. Show an invalid sample without modifying or deleting it.
SELECT kafka_offset, rejection_reason, raw_json
FROM signalwatch.silver.rejected_events_v2
ORDER BY rejected_at DESC
LIMIT 20;

-- 5. Inspect repeated upstream events with different Kafka coordinates.
SELECT event_id, kafka_partition, kafka_offset, duplicate_reason
FROM signalwatch.silver.duplicate_occurrences
ORDER BY kafka_offset DESC
LIMIT 20;

-- 6. Track observed JSON shapes and our explicit parser contract version.
--    A new fingerprint is *not automatically a breaking change*.
SELECT schema_version, shape_fingerprint, unknown_fields_json,
       sample_rejection_reason, first_seen_at
FROM signalwatch.silver.schema_observations
ORDER BY first_seen_at DESC;

-- 7. Show accepted events carrying previously unmodeled keys.
SELECT event_id, schema_version, shape_fingerprint, content_text,
       source_metadata_json
FROM signalwatch.silver.canonical_events_v2
WHERE get_json_object(source_metadata_json, '$.unknown_fields') <> '[]'
LIMIT 20;

-- 8. See canonical Bluesky events ready for later Gold analytics.
SELECT event_time, event_type, entity_id, content_text, language_codes,
       schema_version
FROM signalwatch.silver.canonical_events_v2
ORDER BY event_time DESC
LIMIT 20;

-- 9. Detect same logical commit arriving with different payload bytes.
--    Not every hash difference is an error; review source-replay anomalies.
SELECT d.event_id, c.payload_sha256 AS chosen_payload,
       d.payload_sha256 AS duplicate_payload,
       d.kafka_partition, d.kafka_offset
FROM signalwatch.silver.duplicate_occurrences AS d
JOIN signalwatch.silver.canonical_events_v2 AS c
  ON d.event_id = c.event_id
WHERE d.payload_sha256 <> c.payload_sha256;
