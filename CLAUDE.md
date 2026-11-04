# CLAUDE.md

## Project

Task Dependency Service — FastAPI service that ingests tasks from Kafka and exposes a dependency chain endpoint.

## Stack

- Python 3.12 + FastAPI
- MongoDB (motor) for persistence
- Redis for caching dependency chains
- Kafka (aiokafka) for task event ingestion

## Running locally

```bash
docker-compose up -d        # start Kafka, MongoDB, Redis
pip install -e ".[dev]"     # install all dependencies
uvicorn app.main:app --reload
```

App runs on http://localhost:8000. If port 8000 is taken, use `--port 8001`.

## Key files

- `app/main.py` — FastAPI entrypoint
- `app/models.py` — Task Pydantic model
- `app/db.py` — MongoDB and Redis clients
- `app/routes/tasks.py` — HTTP endpoints
- `app/config.py` — settings via pydantic-settings / .env

## Known issues & fixes

- `pyproject.toml` originally had `build-backend = "setuptools.backends.legacy:build"` which is invalid. Fixed to `setuptools.build_meta`.
- `bitnami/kafka:3.7` is no longer free on Docker Hub. Workaround: pull `apache/kafka:3.7.0` and retag it locally: `docker pull apache/kafka:3.7.0 && docker tag apache/kafka:3.7.0 bitnami/kafka:3.7`

## Tests

```bash
pytest
```

No tests exist yet — pytest and httpx are installed via the `[dev]` extra.