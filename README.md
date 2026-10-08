# SignalWatch

SignalWatch is a near-real-time cross-platform event intelligence data
engineering project.

## Current status

Sprint S0 - local development foundation.

Current infrastructure:

- Python 3.12
- Docker Compose
- Apache Kafka in KRaft mode
- structured logging
- centralized configuration
- common exception hierarchy
- Kafka producer/consumer smoke tests

## Setup

Create and activate the environment:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip setuptools wheel
python -m pip install -e ".[dev]"