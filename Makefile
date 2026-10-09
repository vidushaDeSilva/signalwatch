# Makefile
# Provides short, repeatable commands for common SignalWatch development tasks.

PYTHON := .signalwatch/bin/python
PIP := .signalwatch/bin/pip

.PHONY: install test lint format infra-up infra-down infra-logs kafka-topics producer consumer

install:
	$(PIP) install -e ".[dev]"

test:
	$(PYTHON) -m pytest

lint:
	$(PYTHON) -m ruff check .

format:
	$(PYTHON) -m ruff format .

infra-up:
	docker compose up -d

infra-down:
	docker compose down

infra-logs:
	docker compose logs -f kafka

kafka-topics:
	docker compose exec kafka \
		/opt/kafka/bin/kafka-topics.sh \
		--bootstrap-server kafka:29092 \
		--list

producer:
	$(PYTHON) collectors/smoke_producer.py

consumer:
	$(PYTHON) kafka/smoke_consumer.py

# Start the S1 live Bluesky collector.
bluesky:
	$(PYTHON) collectors/bluesky_collector.py

# S3: Consume Kafka events into recoverable local batches.
landing-consumer:
	$(PYTHON) -m landing.bluesky_landing_consumer

# Preview eligible temporary and uploaded-archive cleanup.
landing-cleanup:
	$(PYTHON) -m landing.cleanup

# Apply safe landing cleanup.
landing-cleanup-apply:
	$(PYTHON) -m landing.cleanup --apply

# S4: Preview landing batches without calling Databricks.
databricks-upload-dry-run:
	$(PYTHON) -m landing.databricks_uploader --once --dry-run

# S4: Attempt a bounded number of ready batches, then exit.
databricks-upload-once:
	$(PYTHON) -m landing.databricks_uploader --once

# S4: Continuously upload newly completed landing batches.
databricks-upload:
	$(PYTHON) -m landing.databricks_uploader