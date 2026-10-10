# Databricks notebook source
"""S8: Correct event-time 5-minute Gold aggregates with late-data correction.

Reads S7 signalwatch.silver.canonical_events_v2. Uses bounded Spark DataFrames
and SQL MERGE, which work on Databricks Serverless. Runs AFTER the S7 task.
Run gold_setup_s8.sql before executing this notebook.
"""

from datetime import datetime, timezone
from uuid import uuid4

from pyspark.sql import Window, functions as F
from pyspark.sql.types import StructField, StructType, StringType, LongType, TimestampType

SOURCE_TABLE = "signalwatch.silver.canonical_events_v2"
GOLD_TABLE = "signalwatch.gold.bluesky_posts_5m"
SOURCE = "bluesky"
WINDOW_DURATION = "5 minutes"
OBSERVED_VERY_LATE_SECONDS = 600  # Observable delay threshold, NOT a streaming watermark.
FUTURE_CLOCK_SKEW_SECONDS = 300

# The S7 notebook already uses UTC; preserve it for consistent SQL windows.
spark.sql("SET TIME ZONE 'UTC'")

# Fields that make a window materially different. Excluding the update
# timestamp ensures replay leaves Gold rows unchanged when input is unchanged.
MEASURE_COLUMNS = [
    "post_count", "late_post_count", "very_late_post_count",
    "out_of_order_post_count", "future_dated_post_count",
    "unknown_ingestion_time_count", "max_ingestion_delay_seconds",
    "last_event_time", "latest_bronze_ingested_at",
]


def posts_from_silver(silver_df):
    """Retain only canonical post *create* events; not updates or deletes."""
    return silver_df.filter(
        (F.col("source") == SOURCE)
        & (F.col("event_type") == "content_created")
        & (F.col("entity_type") == "post")
    )


def validate_silver_posts(posts):
    """Fail rather than silently count duplicate IDs or missing event time."""
    missing = posts.filter(F.col("event_id").isNull() | F.col("event_time").isNull()).limit(1).count()
    if missing:
        raise ValueError("S8 requires non-null event_id and event_time in canonical Silver")

    duplicated = (
        posts.groupBy("event_id").count()
        .filter(F.col("count") > 1)
        .limit(1).count()
    )
    if duplicated:
        raise ValueError("S8 received duplicate logical event_id values from Silver; repair S7 first")


def build_gold_windows(silver_df):
    """Recompute event-time windows irrespective of ingestion/order delays.

    `bronze_ingested_at` is the warehouse-ingestion timestamp, not Kafka
    arrival. The 'late' metric means arrival after the *window end*, while
    'very late' means 10+ minutes between event and Bronze ingestion.
    These metrics are observations; production Gold does NOT drop late data.
    """
    posts = posts_from_silver(silver_df)
    validate_silver_posts(posts)

    # Kafka-offset sequence is used only for the out-of-order diagnostic.
    # Window assignment itself is independent of the order of arrivals.
    previous_events = Window.partitionBy("kafka_topic", "kafka_partition").orderBy(
        F.col("kafka_offset").asc()
    ).rowsBetween(Window.unboundedPreceding, -1)

    with_metrics = (
        posts
        .withColumn("previous_max_event_time", F.max("event_time").over(previous_events))
        .withColumn(
            "is_out_of_order",
            F.when(F.col("previous_max_event_time") > F.col("event_time"), F.lit(1))
            .otherwise(F.lit(0)),
        )
        .withColumn("time_window", F.window("event_time", WINDOW_DURATION))
        .withColumn("window_start", F.col("time_window.start"))
        .withColumn("window_end", F.col("time_window.end"))
        .withColumn(
            "observed_delay_seconds",
            (F.unix_micros("bronze_ingested_at") - F.unix_micros("event_time")) / F.lit(1_000_000.0),
        )
        .withColumn(
            "is_late",
            F.when(F.col("bronze_ingested_at") >= F.col("window_end"), 1).otherwise(0),
        )
        .withColumn(
            "is_very_late",
            F.when(F.col("observed_delay_seconds") > OBSERVED_VERY_LATE_SECONDS, 1).otherwise(0),
        )
        .withColumn(
            "is_future_dated",
            F.when(F.col("observed_delay_seconds") < -FUTURE_CLOCK_SKEW_SECONDS, 1).otherwise(0),
        )
        .withColumn(
            "missing_ingestion_time",
            F.when(F.col("bronze_ingested_at").isNull(), 1).otherwise(0),
        )
    )

    aggregates = (
        with_metrics.groupBy("source", "window_start", "window_end")
        .agg(
            F.count("event_id").cast("long").alias("post_count"),
            F.sum("is_late").cast("long").alias("late_post_count"),
            F.sum("is_very_late").cast("long").alias("very_late_post_count"),
            F.sum("is_out_of_order").cast("long").alias("out_of_order_post_count"),
            F.sum("is_future_dated").cast("long").alias("future_dated_post_count"),
            F.sum("missing_ingestion_time").cast("long").alias("unknown_ingestion_time_count"),
            F.max(F.greatest(F.lit(0.0), F.col("observed_delay_seconds")))
            .alias("max_ingestion_delay_seconds"),
            F.max("event_time").alias("last_event_time"),
            F.max("bronze_ingested_at").alias("latest_bronze_ingested_at"),
        )
        .withColumn("aggregate_updated_at", F.current_timestamp())
        .select(
            "source", "window_start", "window_end", *MEASURE_COLUMNS,
            "aggregate_updated_at",
        )
    )
    return aggregates


def merge_gold_windows(snapshot, table_name=GOLD_TABLE):
    """Converge Gold to a complete source snapshot, including corrections.

    The source DataFrame is a *full* snapshot of the source's windows.
    Do not use this method on a partial incremental update without changing
    the NOT MATCHED BY SOURCE deletion rule.
    """
    if table_name != GOLD_TABLE and not __import__("re").fullmatch(
        r"signalwatch\.gold\._s8_test_[0-9a-f]{10}", table_name
    ):
        raise ValueError(f"Unexpected Gold destination: {table_name}")

    view = f"s8_complete_snapshot_{uuid4().hex}"
    snapshot.createOrReplaceTempView(view)
    # Null-safe comparisons avoid timestamp churn on identical scheduled runs.
    same_measures = " AND ".join(
        f"target.{column} <=> incoming.{column}" for column in MEASURE_COLUMNS
    )
    all_cols = ["source", "window_start", "window_end", *MEASURE_COLUMNS, "aggregate_updated_at"]
    updates = ",\n                ".join(
        f"target.{column} = incoming.{column}" for column in [*MEASURE_COLUMNS, "aggregate_updated_at"]
    )
    inserts = ", ".join(all_cols)
    values = ", ".join(f"incoming.{column}" for column in all_cols)

    try:
        spark.sql(f"""
            MERGE INTO {table_name} AS target
            USING {view} AS incoming
            ON target.source = incoming.source
               AND target.window_start = incoming.window_start
               AND target.window_end = incoming.window_end
            WHEN MATCHED AND NOT ({same_measures}) THEN UPDATE SET
                {updates}
            WHEN NOT MATCHED THEN INSERT ({inserts}) VALUES ({values})
            WHEN NOT MATCHED BY SOURCE AND target.source = '{SOURCE}' THEN DELETE
        """)
    finally:
        spark.catalog.dropTempView(view)


def run_gold_pipeline():
    """Build a complete Gold snapshot and update only changed windows."""
    silver_df = spark.table(SOURCE_TABLE)
    posts = posts_from_silver(silver_df)
    source_count = posts.count()
    existing_count = spark.table(GOLD_TABLE).filter(F.col("source") == SOURCE).count()
    if source_count == 0 and existing_count:
        # An accidentally empty source could otherwise remove historical Gold.
        raise RuntimeError("Nonempty Gold but zero canonical posts; refusing destructive reconciliation")

    snapshot = build_gold_windows(silver_df)
    window_count = snapshot.count()
    merge_gold_windows(snapshot)

    actual_count = (
        spark.table(GOLD_TABLE)
        .filter(F.col("source") == SOURCE)
        .agg(F.coalesce(F.sum("post_count"), F.lit(0)).alias("total"))
        .first()["total"]
    )
    if actual_count != source_count:
        raise AssertionError(
            f"Gold reconciliation failed: {actual_count} posts across windows; "
            f"{source_count} canonical post creates"
        )
    print(f"S8 complete: {source_count} canonical posts in {window_count} 5-minute windows")
    print("Late posts are attributed to their ORIGINAL event-time windows; Gold is corrected on replay.")


def run_s8_gold_self_test():
    """Test 10:01→10:08 late arrival and update/merge idempotency in isolation."""
    from uuid import uuid4

    table_name = f"signalwatch.gold._s8_test_{uuid4().hex[:10]}"
    schema = StructType([
        StructField("event_id", StringType()),
        StructField("source", StringType()),
        StructField("event_type", StringType()),
        StructField("entity_type", StringType()),
        StructField("event_time", TimestampType()),
        StructField("bronze_ingested_at", TimestampType()),
        StructField("kafka_topic", StringType()),
        StructField("kafka_partition", LongType()),
        StructField("kafka_offset", LongType()),
    ])

    def timestamp(hour, minute):
        return datetime(2026, 10, 9, hour, minute, tzinfo=timezone.utc)

    def row(id_, hour, minute, ingest_h, ingest_m, offset):
        return (id_, SOURCE, "content_created", "post", timestamp(hour, minute),
                timestamp(ingest_h, ingest_m), "signalwatch.bluesky.posts.v1", 0, offset)

    original = [row("e1", 10, 1, 10, 2, 1), row("e2", 10, 8, 10, 8, 2)]
    with_late = [*original, row("e3", 10, 1, 10, 8, 3)]
    # Out-of-order event e3 must be included in the original 10:00 window.
    def at_1000():
        matches = spark.table(table_name).filter(
            F.col("window_start") == F.lit(timestamp(10, 0))
        ).collect()
        assert len(matches) == 1, f"Expected one Gold window at 10:00, got {matches}"
        return matches[0]

    created = False
    try:
        spark.sql(f"CREATE TABLE {table_name} USING DELTA AS SELECT * FROM {GOLD_TABLE} WHERE 1 = 0")
        created = True

        merge_gold_windows(build_gold_windows(spark.createDataFrame(original, schema)), table_name)
        before = at_1000()
        assert before.post_count == 1 and before.late_post_count == 0
        first_update = before.aggregate_updated_at

        snapshot = build_gold_windows(spark.createDataFrame(with_late, schema))
        merge_gold_windows(snapshot, table_name)
        after = at_1000()
        assert after.post_count == 2, f"Expected 2, got {after.post_count}"
        assert after.late_post_count == 1, "10:01→10:08 should be late after 10:05"
        assert after.out_of_order_post_count == 1, "Late e3 arrives behind 10:08 in Kafka"
        assert after.aggregate_updated_at >= first_update

        merge_gold_windows(snapshot, table_name)  # replay same complete snapshot
        replay = at_1000()
        assert replay.post_count == 2 and replay.late_post_count == 1
        assert replay.aggregate_updated_at == after.aggregate_updated_at
        assert spark.table(table_name).agg(F.sum("post_count")).first()[0] == 3
        print("PASS S8 Gold self-test: 10:01 arriving 10:08 corrected 10:00–10:05 window; replay unchanged")
    finally:
        if created:
            spark.sql(f"DROP TABLE IF EXISTS {table_name}")


if __name__ == "__main__":
    run_gold_pipeline()
