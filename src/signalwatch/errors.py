"""
Shared exception types for SignalWatch.

Using project-specific exceptions allows callers to distinguish expected
pipeline failures from unexpected Python/runtime errors.
"""


class SignalWatchError(Exception):
    """Base exception for known SignalWatch application errors."""


class ConfigurationError(SignalWatchError):
    """Raised when required application configuration is invalid."""


class KafkaError(SignalWatchError):
    """Base exception for SignalWatch Kafka failures."""


class KafkaConnectionError(KafkaError):
    """Raised when the application cannot communicate with Kafka."""


class KafkaPublishError(KafkaError):
    """Raised when a Kafka message cannot be published successfully."""
