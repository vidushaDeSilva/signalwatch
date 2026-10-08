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