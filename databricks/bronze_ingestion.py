
# Databricks notebook source
"""
S5: Incrementally ingest SignalWatch landing batches into Delta Bronze.

Discovers complete JSONL batches in a Unity Catalog Volume,
validates each manifest and data checksum, reads events with Spark,
inserts previously unseen Kafka records, and records completed batches.

Run this notebook inside Databricks using serverless compute.
No local Spark or Kafka connection is required.
"""

import json
import re

from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)


# ------------------------------------------------------------
# Configuration
# ------------------------------------------------------------

CATALOG = "signalwatch"
SCHEMA = "bronze"

SOURCE_ROOT = (
    f"/Volumes/{CATALOG}/{SCHEMA}/landing/bluesky"
)

EVENTS_TABLE = (
    f"{CATALOG}.{SCHEMA}.bluesky_events"
)

BATCH_LOG_TABLE = (
    f"{CATALOG}.{SCHEMA}.bluesky_ingested_batches"
)

KAFKA_TOPIC = "signalwatch.bluesky.posts.v1"

# Limit each job run to control Free Edition compute usage.
MAX_BATCHES_PER_RUN = 20

# S3 currently creates small JSONL batches.
# Larger files will fail safely until we support chunked verification.
MAX_DATA_FILE_BYTES = 16 * 1024 * 1024
MAX_MANIFEST_BYTES = 64 * 1024

BATCH_NAME_PATTERN = re.compile(
    r"^bluesky_p(\d+)_o(\d+)-(\d+)$"
)


# ------------------------------------------------------------
# Explicit Spark schema
# ------------------------------------------------------------

# Use a fixed schema instead of inferring column types
# independently for every incoming JSONL batch.

LANDING_SCHEMA = StructType([
    StructField("source", StringType(), True),
    StructField("kafka_topic", StringType(), True),
    StructField("kafka_partition", IntegerType(), True),
    StructField("kafka_offset", LongType(), True),
    StructField("kafka_key", StringType(), True),
    StructField("source_time_us", LongType(), True),
    StructField("landed_at", StringType(), True),
    StructField("raw_json", StringType(), True),
])

BATCH_LOG_SCHEMA = StructType([
    StructField("source_batch_id", StringType(), False),
    StructField("landing_file_path", StringType(), False),
    StructField("events_sha256", StringType(), False),
    StructField("record_count", LongType(), False),
])


# ------------------------------------------------------------
# File discovery and manifest validation
# ------------------------------------------------------------

def list_completed_batches():
    """
    List batch directories containing a completion manifest.

    A directory without manifest.json is treated as an
    incomplete S4 upload and is not processed.
    """

    completed = []

    for entry in dbutils.fs.ls(SOURCE_ROOT):
        if not entry.isDir():
            continue

        batch_id = entry.name.rstrip("/")

        if not BATCH_NAME_PATTERN.fullmatch(batch_id):
            continue

        batch_dir = f"{SOURCE_ROOT}/{batch_id}"

        files = {
            item.name: item
            for item in dbutils.fs.ls(batch_dir)
            if not item.isDir()
        }

        if "manifest.json" not in files:
            print(f"SKIP incomplete batch: {batch_id}")
            continue

        if "events.jsonl" not in files:
            raise ValueError(
                f"Committed batch is missing data: {batch_id}"
            )

        completed.append((batch_id, batch_dir, files))

    return sorted(completed, key=lambda item: item[0])


def read_manifest(batch_id, batch_dir, files):
    """Read and validate the S3 completion manifest."""

    manifest_info = files["manifest.json"]

    if manifest_info.size > MAX_MANIFEST_BYTES:
        raise ValueError(
            f"Manifest exceeds size limit: {batch_id}"
        )

    manifest_path = f"{batch_dir}/manifest.json"

    manifest = json.loads(
        dbutils.fs.head(
            manifest_path,
            MAX_MANIFEST_BYTES + 1,
        )
    )

    required = (
        "batch_id",
        "kafka_topic",
        "kafka_partition",
        "record_count",
        "size_bytes",
        "sha256",
    )

    if any(field not in manifest for field in required):
        raise ValueError(
            f"Manifest missing required fields: {batch_id}"
        )

    match = BATCH_NAME_PATTERN.fullmatch(batch_id)

    partition = int(match.group(1))
    start_offset = int(match.group(2))
    end_offset = int(match.group(3))

    if manifest["batch_id"] != batch_id:
        raise ValueError("Batch ID mismatch")

    if manifest["kafka_topic"] != KAFKA_TOPIC:
        raise ValueError("Unexpected Kafka topic")

    if (
        type(manifest["kafka_partition"]) is not int
        or manifest["kafka_partition"] != partition
    ):
        raise ValueError("Kafka partition mismatch")

    if (
        type(manifest["record_count"]) is not int
        or manifest["record_count"] <= 0
    ):
        raise ValueError("Invalid manifest record count")

    if (
        type(manifest["size_bytes"]) is not int
        or manifest["size_bytes"] <= 0
    ):
        raise ValueError("Invalid manifest file size")

    checksum = manifest["sha256"]

    if (
        not isinstance(checksum, str)
        or not re.fullmatch(r"[0-9a-f]{64}", checksum)
    ):
        raise ValueError("Invalid manifest SHA-256")

    if start_offset > end_offset:
        raise ValueError("Invalid batch offset range")

    return manifest, partition, start_offset, end_offset


def verify_remote_data(data_path, expected_hash, expected_size):
    """
    Verify remote JSONL bytes using Spark's binaryFile reader.

    This method is intentionally limited to small S3 batches:
    binaryFile loads the content of each file as a binary value.
    """

    file_size = dbutils.fs.ls(
        data_path.rsplit("/", 1)[0]
    )

    matches = [
        item
        for item in file_size
        if item.name == "events.jsonl"
    ]

    if len(matches) != 1:
        raise ValueError(f"Data file missing: {data_path}")

    actual_size = matches[0].size

    if actual_size != expected_size:
        raise ValueError(
            f"Data file size mismatch: {data_path}"
        )

    if actual_size > MAX_DATA_FILE_BYTES:
        raise ValueError(
            f"Batch too large for S5 verification: {data_path}"
        )

    # Spark reads the remote file bytes and calculates SHA-256.
    result = (
        spark.read
        .format("binaryFile")
        .load(data_path)
        .select(
            F.sha2("content", 256).alias("checksum"),
            F.length("content").alias("size_bytes"),
        )
        .first()
    )

    if result is None:
        raise ValueError(f"Cannot read data file: {data_path}")

    if (
        result["checksum"] != expected_hash
        or result["size_bytes"] != expected_size
    ):
        raise ValueError(
            f"Remote checksum mismatch: {data_path}"
        )


# ------------------------------------------------------------
# Spark DataFrame ingestion
# ------------------------------------------------------------

def load_and_validate_events(
    data_path,
    manifest,
    partition,
    start_offset,
    end_offset,
):
    """
    Read a JSONL batch with Spark and validate Kafka metadata.

    Raw payloads remain untouched. No Silver transformations,
    text cleaning, or filtering are performed.
    """

    df = (
        spark.read
        .schema(LANDING_SCHEMA)
        .option("mode", "FAILFAST")
        .json(data_path)
    )

    invalid = (
        F.col("source").isNull()
        | (F.col("source") != F.lit("bluesky"))
        | F.col("kafka_topic").isNull()
        | (F.col("kafka_topic") != F.lit(KAFKA_TOPIC))
        | F.col("kafka_partition").isNull()
        | (
            F.col("kafka_partition")
            != F.lit(partition)
        )
        | F.col("kafka_offset").isNull()
        | (F.col("kafka_offset") < 0)
        | F.col("raw_json").isNull()
        | (F.length(F.col("raw_json")) == 0)
    )

    # One aggregate pass validates the entire file.
    stats = (
        df.agg(
            F.count("*").alias("records"),
            F.countDistinct(
                "kafka_offset"
            ).alias("distinct_offsets"),
            F.min("kafka_offset").alias("min_offset"),
            F.max("kafka_offset").alias("max_offset"),
            F.sum(
                F.when(invalid, 1).otherwise(0)
            ).alias("invalid_records"),
        )
        .first()
    )

    if stats["records"] != manifest["record_count"]:
        raise ValueError("Record count mismatch")

    if stats["distinct_offsets"] != stats["records"]:
        raise ValueError("Duplicate Kafka offsets within batch")

    if (
        stats["min_offset"] != start_offset
        or stats["max_offset"] != end_offset
    ):
        raise ValueError("Batch offset range mismatch")

    if stats["invalid_records"] != 0:
        raise ValueError("Invalid Kafka record metadata")

    return df


def merge_events(df, batch_id, data_path):
    """
    Insert events not already present in the Bronze Delta table.

    Kafka topic + partition + offset is the deduplication key.
    Existing records are not overwritten.
    """

    bronze_df = (
        df
        .withColumn(
            "source_batch_id",
            F.lit(batch_id),
        )
        .withColumn(
            "landing_file_path",
            F.lit(data_path),
        )
        .withColumn(
            "bronze_ingested_at",
            F.current_timestamp(),
        )
    )

    target = DeltaTable.forName(
        spark,
        EVENTS_TABLE,
    )

    (
        target.alias("target")
        .merge(
            bronze_df.alias("incoming"),
            """
            target.kafka_topic = incoming.kafka_topic
            AND target.kafka_partition = incoming.kafka_partition
            AND target.kafka_offset = incoming.kafka_offset
            """,
        )
        .whenNotMatchedInsertAll()
        .execute()
    )


def record_completed_batch(batch_id, data_path, manifest):
    """Persist a batch completion record after its event merge."""

    log_df = spark.createDataFrame(
        [(
            batch_id,
            data_path,
            manifest["sha256"],
            manifest["record_count"],
        )],
        schema=BATCH_LOG_SCHEMA,
    ).withColumn(
        "processed_at",
        F.current_timestamp(),
    )

    (
        log_df.write
        .format("delta")
        .mode("append")
        .saveAsTable(BATCH_LOG_TABLE)
    )


def ingest_batch(batch_id, batch_dir, files):
    """
    Validate and ingest one completed S4 batch.

    The batch log is written after the event merge so that
    replay remains safe if a job fails between those writes.
    """

    manifest, partition, start, end = read_manifest(
        batch_id,
        batch_dir,
        files,
    )

    data_path = f"{batch_dir}/events.jsonl"

    verify_remote_data(
        data_path=data_path,
        expected_hash=manifest["sha256"],
        expected_size=manifest["size_bytes"],
    )

    df = load_and_validate_events(
        data_path,
        manifest,
        partition,
        start,
        end,
    )

    merge_events(
        df,
        batch_id,
        data_path,
    )

    record_completed_batch(
        batch_id,
        data_path,
        manifest,
    )

    print(
        f"SUCCESS batch={batch_id} "
        f"validated_records={manifest['record_count']}"
    )


# ------------------------------------------------------------
# Incremental job entry point
# ------------------------------------------------------------

def run_bronze_pipeline():
    """
    Process new batches, skipping IDs already in the Delta log.

    Fail the job if any committed batch cannot be validated
    or ingested. Never silently discard corrupted batches.
    """

    print(f"Source: {SOURCE_ROOT}")
    print(f"Target: {EVENTS_TABLE}")

    completed_batches = list_completed_batches()

    # This in-memory lookup is fine for our early-stage project.
    # At larger scale, replace it with a distributed checkpoint lookup.
    processed_ids = {
        row["source_batch_id"]
        for row in (
            spark.table(BATCH_LOG_TABLE)
            .select("source_batch_id")
            .collect()
        )
    }

    new_batches = [
        batch
        for batch in completed_batches
        if batch[0] not in processed_ids
    ]

    selected = new_batches[:MAX_BATCHES_PER_RUN]

    print(
        f"Complete batches: {len(completed_batches)}, "
        f"pending: {len(new_batches)}, "
        f"processing now: {len(selected)}"
    )

    for batch_id, batch_dir, files in selected:
        ingest_batch(batch_id, batch_dir, files)

    print(
        f"Bronze run finished. "
        f"Batches processed: {len(selected)}"
    )


run_bronze_pipeline()
