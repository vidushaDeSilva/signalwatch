# Databricks notebook source
"""S7: Idempotent Bluesky Bronze-to-Silver normalization with quarantine.

Databricks serverless notebook. Keep silver_contract.py as a workspace Python
file in the same folder as this notebook, and execute silver_setup_s7.sql first.

IMPORTANT: Writes to *_v2 tables, leaving existing S6 tables untouched.
The S6 task should be replaced by this notebook in the Databricks Job.
"""

from delta.tables import DeltaTable
from pyspark.sql import Window, functions as F
from pyspark.sql.types import (
    ArrayType, IntegerType, LongType, StringType, StructField, StructType,
)

from silver_contract import CONTRACT_VERSION, normalize_bluesky_event

BRONZE_TABLE = "signalwatch.bronze.bluesky_events"
SILVER_TABLE = "signalwatch.silver.canonical_events_v2"
REJECTED_TABLE = "signalwatch.silver.rejected_events_v2"
DUPLICATE_TABLE = "signalwatch.silver.duplicate_occurrences"
SHAPES_TABLE = "signalwatch.silver.schema_observations"

# Interpret/display timestamps consistently with the source's UTC instants.
spark.sql("SET TIME ZONE 'UTC'")

VALIDATION_SCHEMA = StructType([
    StructField("rejection_reason", StringType(), True),
    StructField("event_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("event_time_us", LongType(), True),
    StructField("actor_id", StringType(), True),
    StructField("entity_id", StringType(), True),
    StructField("entity_type", StringType(), True),
    StructField("content_text", StringType(), True),
    StructField("language_codes", ArrayType(StringType()), True),
    StructField("content_created_at_us", LongType(), True),
    StructField("source_metadata_json", StringType(), True),
    StructField("schema_version", StringType(), True),
    StructField("shape_fingerprint", StringType(), True),
    StructField("unknown_fields_json", StringType(), True),
])

# Python UDF gives explicit, per-record JSON type validation. It is simple to
# test locally and adequate for current small files, but costs Python/Spark
# serialization per row. For large volumes, use native Spark/VARIANT parsing.
validate_udf = F.udf(normalize_bluesky_event, VALIDATION_SCHEMA)


def merge_new(table_name, source_df, identity_column):
    """
    Insert unseen records using SQL MERGE on Databricks Serverless.

    Supports the four production tables and S7's isolated test tables.
    Validates identifiers before inserting them into SQL.
    """
    import re
    from uuid import uuid4

    production_tables = {
        SILVER_TABLE,
        REJECTED_TABLE,
        DUPLICATE_TABLE,
        SHAPES_TABLE,
    }

    # Self-test creates tables named:
    # signalwatch.silver._s7_test_<index>_<10-character hex ID>
    test_table_pattern = (
        r"signalwatch\.silver\._s7_test_[0-3]_[0-9a-f]{10}"
    )

    valid_test_table = re.fullmatch(test_table_pattern, table_name) is not None

    if table_name not in production_tables and not valid_test_table:
        raise ValueError(f"Unexpected target table: {table_name}")

    allowed_identity_columns = {
        "event_id",
        "occurrence_id",
        "shape_fingerprint",
    }

    if identity_column not in allowed_identity_columns:
        raise ValueError(f"Unexpected identity column: {identity_column}")

    view_name = f"s7_merge_{uuid4().hex}"

    source_df.createOrReplaceTempView(view_name)

    try:
        spark.sql(f"""
            MERGE INTO {table_name} AS target
            USING {view_name} AS incoming
            ON target.{identity_column} = incoming.{identity_column}
            WHEN NOT MATCHED THEN INSERT *
        """)
    finally:
        spark.catalog.dropTempView(view_name)



def load_bronze():
    """Read original records, preserving a separate Kafka occurrence ID."""
    bronze = (
        spark.table(BRONZE_TABLE)
        .filter(F.col("source") == "bluesky")
        .select(
            "source", "kafka_topic", "kafka_partition", "kafka_offset",
            "source_time_us", "raw_json", "source_batch_id",
            "bronze_ingested_at",
        )
    )

    # A Kafka occurrence remains distinct even when the upstream commit
    # appeared on more than one offset.
    coordinates = F.struct(
        F.col("source"), F.col("kafka_topic"),
        F.col("kafka_partition"), F.col("kafka_offset"),
    )
    return (
        bronze
        .withColumn("occurrence_id", F.sha2(F.to_json(coordinates), 256))
        .withColumn("payload_sha256", F.sha2("raw_json", 256))
        .dropDuplicates(["occurrence_id"])
    )


def build_outputs(bronze):
    """Return canonical, quarantine, duplicate and schema-audit DataFrames."""
    staged = bronze.withColumn(
        "quality",
        validate_udf(F.col("raw_json"), F.col("source_time_us")),
    )

    good = staged.filter(F.col("quality.rejection_reason").isNull())
    bad = staged.filter(F.col("quality.rejection_reason").isNotNull())

    # Pick one Kafka occurrence per *source-level* commit, independent of
    # whether that commit arrived at multiple Kafka offsets.
    winner_order = Window.partitionBy("quality.event_id").orderBy(
        F.col("kafka_topic").asc(),
        F.col("kafka_partition").asc(),
        F.col("kafka_offset").asc(),
    )
    ranked = good.withColumn("_rank", F.row_number().over(winner_order))
    winners = ranked.filter(F.col("_rank") == 1)
    repeated = ranked.filter(F.col("_rank") > 1)

    canonical = winners.select(
        F.col("quality.event_id").alias("event_id"),
        "source",
        F.col("quality.event_type").alias("event_type"),
        F.expr("timestamp_micros(quality.event_time_us)").alias("event_time"),
        F.col("quality.actor_id").alias("actor_id"),
        F.col("quality.entity_id").alias("entity_id"),
        F.col("quality.entity_type").alias("entity_type"),
        F.col("quality.content_text").alias("content_text"),
        F.col("quality.language_codes").alias("language_codes"),
        F.expr("timestamp_micros(quality.content_created_at_us)").alias("content_created_at"),
        F.col("quality.source_metadata_json").alias("source_metadata_json"),
        "kafka_topic", "kafka_partition", "kafka_offset",
        F.col("source_batch_id").alias("bronze_batch_id"),
        "bronze_ingested_at",
        F.current_timestamp().alias("silver_ingested_at"),
        F.col("quality.schema_version").alias("schema_version"),
        F.col("quality.shape_fingerprint").alias("shape_fingerprint"),
        "occurrence_id",
        "payload_sha256",
    )

    rejected = bad.select(
        "occurrence_id", "source",
        "kafka_topic", "kafka_partition", "kafka_offset",
        F.col("quality.rejection_reason").alias("rejection_reason"),
        F.col("quality.schema_version").alias("schema_version"),
        F.col("quality.shape_fingerprint").alias("shape_fingerprint"),
        F.col("quality.unknown_fields_json").alias("unknown_fields_json"),
        "raw_json",
        F.col("source_batch_id").alias("bronze_batch_id"),
        F.current_timestamp().alias("rejected_at"),
    )

    duplicates = repeated.select(
        "occurrence_id",
        F.col("quality.event_id").alias("event_id"),
        "source", "kafka_topic", "kafka_partition", "kafka_offset",
        "payload_sha256",
        F.lit("repeated_source_commit").alias("duplicate_reason"),
        F.col("source_batch_id").alias("bronze_batch_id"),
        F.current_timestamp().alias("detected_at"),
    )

    # One registry row per *observed structural shape*. Unknown additive
    # fields are accepted but visible. Rejected shapes are tracked too.
    shapes = (
        staged
        .filter(F.col("quality.shape_fingerprint").isNotNull())
        .withColumn(
            "_rank",
            F.row_number().over(
                Window.partitionBy("quality.shape_fingerprint").orderBy(
                    "kafka_topic", "kafka_partition", "kafka_offset",
                )
            ),
        )
        .filter(F.col("_rank") == 1)
        .select(
            F.col("quality.shape_fingerprint").alias("shape_fingerprint"),
            F.col("quality.schema_version").alias("schema_version"),
            F.col("quality.unknown_fields_json").alias("unknown_fields_json"),
            "occurrence_id",
            F.col("quality.rejection_reason").alias("sample_rejection_reason"),
            F.current_timestamp().alias("first_seen_at"),
        )
    )
    return canonical, rejected, duplicates, shapes


def run_silver_pipeline():
    """
    Replay Bronze into S7 Silver, quarantine, duplicate, and schema tables.

    Compatible with Databricks Serverless compute.
    Recomputes the source DataFrame when needed instead of caching it.
    """

    print(f"S7 schema contract: {CONTRACT_VERSION}")

    # Do not use .cache() or .persist() on serverless.
    source = load_bronze()

    count = source.count()

    canonical, rejected, duplicates, shapes = build_outputs(source)

    # Each Delta MERGE is atomic and idempotent independently.
    merge_new(SILVER_TABLE, canonical, "event_id")
    merge_new(REJECTED_TABLE, rejected, "occurrence_id")
    merge_new(DUPLICATE_TABLE, duplicates, "occurrence_id")
    merge_new(SHAPES_TABLE, shapes, "shape_fingerprint")

    print(f"S7 Bronze source occurrences: {count}")

    for table in (
        SILVER_TABLE,
        REJECTED_TABLE,
        DUPLICATE_TABLE,
        SHAPES_TABLE,
    ):
        print(f"{table}: {spark.table(table).count()}")

    print("S7 Silver reliability job completed")


def run_s7_self_test():
    """Exercise six S7 failure scenarios using isolated temporary Delta tables.

    Synthetic events never touch the production Bronze/Silver data. The test
    validates real Spark UDF behavior, semantic deduplication, and rerun-safe
    Delta MERGE. Call manually from a new notebook cell after S7 setup.
    """
    import json
    from copy import deepcopy
    from uuid import uuid4

    time_us = 1_791_540_000_000_000
    base = {
        "did": "did:plc:s7-test-user",
        "kind": "commit",
        "time_us": time_us,
        "commit": {
            "operation": "create", "collection": "app.bsky.feed.post",
            "rkey": "s7-base", "rev": "s7-rev-001", "cid": "bafytest",
            "record": {"text": "replay test", "langs": ["en"],
                       "createdAt": "2026-10-09T12:00:00Z"},
        },
    }

    missing = deepcopy(base)
    del missing["time_us"]
    unknown_kind = deepcopy(base)
    unknown_kind["kind"] = "identity"
    additive = deepcopy(base)
    additive["commit"]["rkey"] = "s7-additive"
    additive["commit"]["rev"] = "s7-rev-002"
    additive["commit"]["record"]["new_source_field"] = {"x": 1}
    changed = deepcopy(base)
    changed["commit"]["record"]["text"] = {"unexpected": "object"}

    # Seven Kafka occurrences: 2 copies of the same source commit,
    # 4 invalid/unsupported records, and 1 additive valid record.
    payloads = [
        json.dumps(base), json.dumps(base), json.dumps(missing),
        "{not valid JSON", json.dumps(unknown_kind),
        json.dumps(additive), json.dumps(changed),
    ]
    rows = [(
        "bluesky", "signalwatch.bluesky.posts.v1", 0, idx,
        time_us, raw, "s7_synthetic_test", None,
    ) for idx, raw in enumerate(payloads)]

    sample_schema = StructType([
        StructField("source", StringType()),
        StructField("kafka_topic", StringType()),
        StructField("kafka_partition", IntegerType()),
        StructField("kafka_offset", LongType()),
        StructField("source_time_us", LongType()),
        StructField("raw_json", StringType()),
        StructField("source_batch_id", StringType()),
        StructField("bronze_ingested_at", StringType()),
    ])
    sample = spark.createDataFrame(rows, sample_schema)
    sample = sample.withColumn(
        "bronze_ingested_at", F.to_timestamp("bronze_ingested_at")
    )
    identity = F.struct("source", "kafka_topic", "kafka_partition", "kafka_offset")
    sample = (
        sample.withColumn("occurrence_id", F.sha2(F.to_json(identity), 256))
        .withColumn("payload_sha256", F.sha2("raw_json", 256))
    )

    canonical, rejected, duplicates, shapes = build_outputs(sample)
    expected_counts = [2, 4, 1]
    actual_counts = [canonical.count(), rejected.count(), duplicates.count()]
    assert actual_counts == expected_counts, (
        f"Expected canonical/rejected/duplicates={expected_counts}; got {actual_counts}"
    )
    assert sum(actual_counts) == sample.count() == 7
    assert (
        canonical.select("event_id").distinct().count()
        == canonical.count()
    )

    from_tables = [SILVER_TABLE, REJECTED_TABLE, DUPLICATE_TABLE, SHAPES_TABLE]
    outputs = [canonical, rejected, duplicates, shapes]
    keys = ["event_id", "occurrence_id", "occurrence_id", "shape_fingerprint"]
    unique = uuid4().hex[:10]
    test_tables = [f"signalwatch.silver._s7_test_{i}_{unique}" for i in range(4)]
    created = []
    try:
        # Clone schema by CTAS without copying any existing user records.
        for source_name, target_name in zip(from_tables, test_tables):
            spark.sql(
                f"CREATE TABLE {target_name} USING DELTA "
                f"AS SELECT * FROM {source_name} WHERE 1 = 0"
            )
            created.append(target_name)

        for target_name, output, key in zip(test_tables, outputs, keys):
            merge_new(target_name, output, key)
        before = [spark.table(name).count() for name in test_tables]

        # Replay the exact same source occurrences to prove idempotency.
        for target_name, output, key in zip(test_tables, outputs, keys):
            merge_new(target_name, output, key)
        after = [spark.table(name).count() for name in test_tables]
        assert before == after, f"Replay inserted duplicates: before={before}, after={after}"
        assert before[:3] == expected_counts
        print(f"PASS S7 Spark + Delta replay self-test: {before}")
    finally:
        for table_name in reversed(created):
            spark.sql(f"DROP TABLE IF EXISTS {table_name}")


# Databricks executes notebook source with __name__ == '__main__'.
if __name__ == "__main__":
    run_silver_pipeline()
