"""
Central configuration for SignalWatch.

This module loads application settings from environment variables and the
local .env file. Other project components should use this module instead of
reading environment variables directly throughout the codebase.
"""

from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application configuration loaded from environment variables."""

    environment: str = Field(default="local", alias="ENVIRONMENT")
    log_level: str = Field(default="INFO", alias="LOG_LEVEL")

    kafka_bootstrap_servers: str = Field(
        default="localhost:9092",
        alias="KAFKA_BOOTSTRAP_SERVERS",
    )

    kafka_test_topic: str = Field(
        default="signalwatch.s0.smoke",
        alias="KAFKA_TEST_TOPIC",
    )

    kafka_test_consumer_group: str = Field(
        default="signalwatch-s0-smoke",
        alias="KAFKA_TEST_CONSUMER_GROUP",
    )

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    # Bluesky Jetstream connection and recovery settings.
    jetstream_url: str = Field(
        default="wss://jetstream2.us-east.bsky.network/subscribe",
        alias="JETSTREAM_URL",
    )

    jetstream_collection: str = Field(
        default="app.bsky.feed.post",
        alias="JETSTREAM_COLLECTION",
    )

    jetstream_cursor_file: str = Field(
        default="data/checkpoints/bluesky_cursor.json",
        alias="JETSTREAM_CURSOR_FILE",
    )

    jetstream_replay_overlap_seconds: int = Field(
        default=5,
        ge=0,
        alias="JETSTREAM_REPLAY_OVERLAP_SECONDS",
    )

    jetstream_cursor_save_every: int = Field(
        default=100,
        ge=1,
        alias="JETSTREAM_CURSOR_SAVE_EVERY",
    )

    jetstream_log_every: int = Field(
        default=100,
        ge=1,
        alias="JETSTREAM_LOG_EVERY",
    )


@lru_cache
def get_settings() -> Settings:
    """Return one cached Settings instance for the running process."""

    return Settings()
