# Databricks notebook source
"""S6: Normalize Bluesky Bronze records into canonical Silver events.

Runs on Databricks serverless Spark. Retains create, update and delete
operations, sends malformed/unsupported records to a rejection table,
and uses insert-only Delta MERGE for safe repeated execution.

Prerequisite: Execute databricks/silver_setup.sql to create Delta tables.
"""

from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, LongType, StringType, StructField, StructType


# The Bronze table was created in S5. These two Silver tables are new.
BRONZE_TABLE = "signalwatch.bronze.bluesky_events"
SILVER_TABLE = "signalwatch.silver.canonical_events"
REJECTED_TABLE = "signalwatch.silver.rejected_events"
POST_COLLECTION = "app.bsky.feed.post"

# Databricks timestamps are instants; setting UTC also makes their display
# and parsing of timestamps without explicit offsets predictable.
spark.sql("SET TIME ZONE 'UTC'")


# Parse only fields needed to normalize Bluesky; all other source JSON
# remains unchanged in the Bronze table for future reinterpretation.
POST_SCHEMA = StructType([
    StructField("text", StringType(), True),
    StructField("createdAt", StringType(), True),
    StructField("langs", ArrayType(StringType()), True),
])

COMMIT_SCHEMA = StructType([
    StructField("operation", StringType(), True),
    StructField("collection", StringType(), True),
    StructField("rkey", StringType(), True),
    StructField("rev", StringType(), True),
    StructField("cid", StringType(), True),
    StructField("record", POST_SCHEMA, True),
])

JETSTREAM_SCHEMA = StructType([
    StructField("did", StringType(), True),
    StructField("time_us", LongType(), True),
    StructField("kind", StringType(), True),
    StructField("commit", COMMIT_SCHEMA, True),
])


def build_staged_events(bronze_df):
    """Parse source-specific Bluesky fields and assign quality failures."""
    parsed = bronze_df.withColumn(
        "payload", F.from_json(F.col("raw_json"), JETSTREAM_SCHEMA)
    )

    operation = F.col("payload.commit.operation")
    did = F.col("payload.did")
    rkey = F.col("payload.commit.rkey")
    record = F.col("payload.commit.record")
    created_at_string = F.col("payload.commit.record.createdAt")
    parsed_created_at = F.try_to_timestamp(created_at_string)

    invalid_reason = (
        F.when(F.col("payload").isNull(), "malformed_json")
        .when(
            F.col("source_time_us").isNull()
            | (F.col("source_time_us") <= 0)
            | (F.col("source_time_us") >= 253402300800000000),
            "invalid_event_time",
        )
        .when(
            F.col("payload.kind").isNull()
            | (F.col("payload.kind") != "commit"),
            "unsupported_event_kind",
        )
        .when(
            F.col("payload.commit.collection").isNull()
            | (F.col("payload.commit.collection") != POST_COLLECTION),
            "unexpected_collection",
        )
        .when(did.isNull() | (F.length(F.trim(did)) == 0), "missing_actor_did")
        .when(rkey.isNull() | (F.length(F.trim(rkey)) == 0), "missing_record_key")
        .when(
            operation.isNull() | (~operation.isin("create", "update", "delete")),
            "unsupported_operation",
        )
        .when(
            operation.isin("create", "update") & record.isNull(),
            "missing_post_record",
        )
        .when(
            created_at_string.isNotNull() & parsed_created_at.isNull(),
            "invalid_content_created_at",
        )
    )

    return parsed.withColumn("rejection_reason", invalid_reason)


def canonicalize_valid_events(staged_df):
    """Produce source-independent Silver columns for valid post events."""
    operation = F.col("payload.commit.operation")
    supplied_langs = F.coalesce(
        F.col("payload.commit.record.langs"),
        F.array().cast("array<string>"),
    )

    # Languages are metadata supplied by the author/client, not detected by us.
    normalized_langs = F.array_distinct(
        F.transform(
            F.filter(
                supplied_langs,
                lambda lang: lang.isNotNull() & (F.length(F.trim(lang)) > 0),
            ),
            lambda lang: F.lower(F.trim(lang)),
        )
    )

    event_type = (
        F.when(operation == "create", "content_created")
        .when(operation == "update", "content_updated")
        .when(operation == "delete", "content_deleted")
    )

    # This reconstructs the record URI, which is shared by its change events.
    entity_id = F.concat(
        F.lit("at://"),
        F.col("payload.did"),
        F.lit("/"),
        F.col("payload.commit.collection"),
        F.lit("/"),
        F.col("payload.commit.rkey"),
    )

    # Source-specific details are extensible without adding Bluesky-only
    # top-level columns to our reusable cross-source schema.
    source_metadata = F.to_json(F.struct(
        F.col("payload.kind").alias("jetstream_kind"),
        operation.alias("operation"),
        F.col("payload.commit.collection").alias("collection"),
        F.col("payload.commit.rkey").alias("rkey"),
        F.col("payload.commit.rev").alias("revision"),
        F.col("payload.commit.cid").alias("cid"),
        F.col("payload.time_us").alias("jetstream_time_us"),
    ))

    return (
        staged_df
        .filter(F.col("rejection_reason").isNull())
        .select(
            "event_id",
            "source",
            event_type.alias("event_type"),
            F.expr("timestamp_micros(source_time_us)").alias("event_time"),
            F.col("payload.did").alias("actor_id"),
            entity_id.alias("entity_id"),
            F.lit("post").alias("entity_type"),
            F.col("payload.commit.record.text").alias("content_text"),
            normalized_langs.alias("language_codes"),
            F.try_to_timestamp(
                F.col("payload.commit.record.createdAt")
            ).alias("content_created_at"),
            source_metadata.alias("source_metadata_json"),
            "kafka_topic",
            "kafka_partition",
            "kafka_offset",
            F.col("source_batch_id").alias("bronze_batch_id"),
            "bronze_ingested_at",
            F.current_timestamp().alias("silver_ingested_at"),
        )
    )


def canonicalize_rejected_events(staged_df):
    """Retain rejected raw events and their validation failure reasons."""
    return (
        staged_df
        .filter(F.col("rejection_reason").isNotNull())
        .select(
            "event_id",
            "source",
            "kafka_topic",
            "kafka_partition",
            "kafka_offset",
            "rejection_reason",
            "raw_json",
            F.current_timestamp().alias("rejected_at"),
        )
    )


def insert_new_events(table_name, events_df):
    """Idempotently insert records based on their Kafka-derived event ID."""
    (
        DeltaTable.forName(spark, table_name)
        .alias("target")
        .merge(events_df.alias("incoming"), "target.event_id = incoming.event_id")
        .whenNotMatchedInsertAll()
        .execute()
    )


def run_silver_pipeline():
    """Reconcile Bronze source events into valid or rejected Silver rows.

    This starter implementation rereads Bronze on each run. MERGE makes
    writes idempotent; a later sprint can use Delta Change Data Feed to
    scan only newly committed Bronze records.
    """
    bronze = (
        spark.table(BRONZE_TABLE)
        .filter(F.col("source") == "bluesky")
        .select(
            "source", "kafka_topic", "kafka_partition", "kafka_offset",
            "source_time_us", "raw_json", "source_batch_id",
            "bronze_ingested_at",
        )
    )

    # S5 already ensures unique Kafka coordinates. Keep a defensive
    # deduplication before each Delta MERGE to avoid duplicate insert matches.
    bronze = (
        bronze
        .withColumn(
            "event_id",
            F.concat_ws(
                ":", "source", "kafka_topic",
                F.col("kafka_partition").cast("string"),
                F.col("kafka_offset").cast("string"),
            ),
        )
        .dropDuplicates(["event_id"])
    )

    staged = build_staged_events(bronze)
    valid = canonicalize_valid_events(staged)
    rejected = canonicalize_rejected_events(staged)

    # Merge valid rows first. If the job stops before merging rejections,
    # a rerun safely completes both tables without duplicating valid rows.
    insert_new_events(SILVER_TABLE, valid)
    insert_new_events(REJECTED_TABLE, rejected)

    print("S6 Silver normalization completed")
    print(f"Canonical events: {spark.table(SILVER_TABLE).count()}")
    print(f"Rejected events: {spark.table(REJECTED_TABLE).count()}")


run_silver_pipeline()
