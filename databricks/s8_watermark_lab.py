# Databricks notebook source
"""S8: Isolated TRUE Structured Streaming watermark experiment on Serverless.

Runs four finite AvailableNow triggers against an append-only Delta test source.
The streaming checkpoint preserves maximum-seen event time and window state.
It never touches SignalWatch production Silver/Bronze/Gold event tables.

Prerequisite: execute gold_setup_s8.sql, including the checkpoint volume.
Execute this notebook manually, NOT as a production Job task.
"""

from uuid import uuid4

from pyspark.sql import functions as F

spark.sql("SET TIME ZONE 'UTC'")


def run_s8_watermark_lab():
    """Contrast accepted late arrivals with too-late data after state eviction."""
    suffix = uuid4().hex[:10]
    source = f"signalwatch.gold._s8_watermark_input_{suffix}"
    target = f"signalwatch.gold._s8_watermark_output_{suffix}"
    checkpoint = f"/Volumes/signalwatch/gold/s8_stream_checkpoints/s8_lab_{suffix}"
    created = []

    def append_test_event(event_id: str, event_at: str, arrived_at: str):
        """Append one record to the true streaming source Delta table."""
        # Fixed, generated test identifiers/timestamps; never substitute
        # untrusted user input into this SQL example.
        spark.sql(f"""
            INSERT INTO {source}
            VALUES ('{event_id}', TIMESTAMP '{event_at}', TIMESTAMP '{arrived_at}')
        """)

    def process_available_now(label: str):
        """Restart the stream from the SAME checkpoint after new test events."""
        updates = (
            spark.readStream.format("delta").table(source)
            .withWatermark("event_time", "10 minutes")
            .groupBy(F.window("event_time", "5 minutes"))
            .count()
            .select(
                F.col("window.start").alias("window_start"),
                F.col("window.end").alias("window_end"),
                F.col("count").alias("post_count"),
            )
        )
        query = (
            updates.writeStream.format("delta")
            .outputMode("append")
            .option("checkpointLocation", checkpoint)
            .trigger(availableNow=True)
            .toTable(target)
        )
        query.awaitTermination()
        if query.exception() is not None:
            raise RuntimeError(f"Streaming watermark lab failed in {label}: {query.exception()}")
        print(f"{label}: processed all currently available Delta changes")

    def ten_oclock_count():
        rows = spark.sql(f"""
            SELECT post_count FROM {target}
            WHERE window_start = TIMESTAMP '2026-10-09 10:00:00'
              AND window_end = TIMESTAMP '2026-10-09 10:05:00'
        """).collect()
        return None if not rows else rows[0][0]

    try:
        spark.sql(f"""
            CREATE TABLE {source} (
                event_id STRING, event_time TIMESTAMP, simulated_arrival TIMESTAMP
            ) USING DELTA
        """)
        created.append(source)
        spark.sql(f"""
            CREATE TABLE {target} (
                window_start TIMESTAMP, window_end TIMESTAMP, post_count BIGINT
            ) USING DELTA
        """)
        created.append(target)

        # First observed maximum event-time 10:08 -> watermark about 09:58.
        append_test_event("e1", "2026-10-09 10:01:00", "2026-10-09 10:01:00")
        append_test_event("e2", "2026-10-09 10:08:00", "2026-10-09 10:08:00")
        process_available_now("Stage A: first events")

        # This 10:01 event ARRIVES at 10:08 (next trigger). Its event time
        # is still within the 10-minute watermark horizon, so it is counted.
        append_test_event("e3", "2026-10-09 10:01:00", "2026-10-09 10:08:00")
        process_available_now("Stage B: accepted 10:01 late arrival")

        # Advance stream-time beyond 10:15 so the 10:00–10:05 window can close.
        append_test_event("e4", "2026-10-09 10:20:00", "2026-10-09 10:20:00")
        process_available_now("Stage C: advance watermark")

        # This 10:02 event is injected AFTER the max has reached 10:20.
        # The 10:00–10:05 window has expired from streaming state.
        append_test_event("e5", "2026-10-09 10:02:00", "2026-10-09 10:25:00")
        process_available_now("Stage D: too-late arrival")

        observed = ten_oclock_count()
        assert observed == 2, (
            f"Expected finalized 10:00–10:05 streaming count 2, observed {observed}. "
            "Inspect the checkpoint and trigger logs before changing the test."
        )
        print("PASS S8 streaming watermark lab: 10:01 at 10:08 counted; 10:02 after state eviction excluded")
        print("Production Gold has a DIFFERENT policy: full-window recomputation also includes e5.")
    finally:
        # Do not remove the checkpoint until streaming queries have terminated.
        for table_name in reversed(created):
            spark.sql(f"DROP TABLE IF EXISTS {table_name}")
        try:
            dbutils.fs.rm(checkpoint, recurse=True)
        except Exception as exc:
            print(f"Checkpoint cleanup needs manual attention: {checkpoint}: {exc}")


if __name__ == "__main__":
    run_s8_watermark_lab()
