# Task Dependency Service

A backend service for a project management tool. Tasks stream in from Kafka, are persisted in MongoDB, and the service exposes a REST API to look up a task's full dependency chain. Dependency chains are cached in Redis.

## Stack

| Component | Technology |
|-----------|------------|
| API | Python 3.12 + FastAPI |
| Persistence | MongoDB (motor) |
| Cache | Redis |
| Event stream | Kafka (aiokafka) |

## Architecture

```mermaid
flowchart TD
    HTTP(["🌐 HTTP Client"])

    subgraph INGEST ["⚡ Ingestion Pipeline"]
        direction TB
        K(["📨 Kafka\ntasks.events"])
        C["🔄 KafkaConsumer\nbackground task"]
        K -->|consume| C
    end

    subgraph STORE ["🗄️ Storage"]
        direction TB
        M[("🍃 MongoDB\ntaskdb.tasks")]
        R[("⚡ Redis\ntds:chain / tds:members")]
    end

    subgraph API ["🚀 API Layer"]
        direction TB
        RT["📋 GET /tasks/{id}"]
        RC["🔗 GET /tasks/{id}/chain"]
        RH["❤️ GET /health"]
        SVC["⚙️ TaskChainService"]
        RC --> SVC
    end

    C -->|upsert| M
    C -->|invalidate| R

    HTTP -->|lookup| RT
    HTTP -->|resolve chain| RC
    HTTP -->|readiness| RH

    RT -->|find_by_id| M
    RH -->|ping| M
    RH -->|ping| R

    SVC -->|cache get| R
    R -->|"✅ hit"| SVC
    SVC -->|"❌ miss: walk parents"| M
    M -->|ancestors| SVC
    SVC -->|"cache set\n(complete + incomplete; never cycle)"| R

    style INGEST fill:#1e3a5f,stroke:#4a90d9,color:#e8f4fd
    style STORE  fill:#1a3a2a,stroke:#4caf50,color:#e8f5e9
    style API    fill:#3a1a2e,stroke:#ce93d8,color:#f3e5f5

    style K  fill:#0d47a1,stroke:#42a5f5,color:#fff
    style C  fill:#1565c0,stroke:#42a5f5,color:#fff
    style M  fill:#1b5e20,stroke:#66bb6a,color:#fff
    style R  fill:#b71c1c,stroke:#ef5350,color:#fff
    style RT fill:#4a148c,stroke:#ce93d8,color:#fff
    style RC fill:#4a148c,stroke:#ce93d8,color:#fff
    style SVC fill:#6a1b9a,stroke:#ce93d8,color:#fff
    style RH  fill:#4a148c,stroke:#ce93d8,color:#fff
    style HTTP fill:#e65100,stroke:#ffa726,color:#fff
```

### Data flow — write path

A task event arrives on Kafka → `KafkaConsumer` deserialises the JSON and upserts the document into MongoDB → Redis cache entries for that task (and all cached chains that include it as an ancestor) are invalidated **before and after** the upsert. The double invalidation closes the race window where a concurrent reader could otherwise cache stale data between the two operations.

### Data flow — read path

`GET /tasks/{id}/chain` → `TaskChainService` checks Redis first. On a cache miss it fetches the full ancestor chain in a single `$graphLookup` aggregation query and writes the result to Redis. **Complete chains** are cached for 1 hour. **Incomplete chains** (missing parent or depth limit hit) are cached for 30 seconds to reduce MongoDB pressure during ingestion lag; they resolve correctly within 30 seconds once the missing parent arrives. Chains with a detected cycle are **never** cached.

---

## 1. Prerequisites

- Docker + Docker Compose
- Python 3.12+

---

## 2. Start Infrastructure

```bash
docker compose up -d
```

> If Kafka fails to pull, see [Known Setup Notes → Kafka image](#kafka-image).

This starts Kafka, MongoDB, and Redis. Verify all three containers are running:

```bash
docker compose ps
```

---

## 3. Install Dependencies

```bash
python -m venv .venv && source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -e ".[dev]"
```

---

## 4. Start the Application

```bash
uvicorn app.main:app --reload
```

The service starts at **http://localhost:8000**.  
Swagger UI: **http://localhost:8000/docs**

If port 8000 is already in use, pass `--port 8001` and update the base URL accordingly.

On startup the app will:
- Create a unique index on `tasks.id` and an index on `tasks.parent_task_id` in MongoDB
- Start the Kafka consumer (background task listening to `tasks.events`)

---

## 5. Run the Tests

```bash
pytest -v
```

Tests use in-memory fakes for MongoDB, Redis, and Kafka — no running infrastructure required.

To check code quality:

```bash
pylint app/
mypy app/ --ignore-missing-imports
```

---

## 6. Seed Tasks (End-to-End Demo)

> Requires the infrastructure from step 2 to be running (`docker compose up -d`).

### Option A — Default sample (3 tasks)

Publishes a chain: `t1 (root) → t2 → t3`

```bash
python scripts/produce_tasks.py
```

### Option B — Large dataset (100 tasks)

Publishes a linear chain of 100 tasks: `t1 → t2 → … → t100`

```bash
python scripts/produce_tasks.py --count 100
```

The running Kafka consumer will ingest all tasks into MongoDB automatically.

> **Note:** With the default depth cap of 100, querying `GET /tasks/t100/chain` returns
> `X-Chain-Complete: false, X-Chain-Depth-Limited: true` because the chain is exactly at
> the limit. Use `--count 10` for a fully-complete chain demo, or raise `MAX_CHAIN_DEPTH`
> in your `.env` file.

### Available flags

| Flag | Default | Description |
|------|---------|-------------|
| `--count N` | *(3-task sample)* | Generate a linear chain of N tasks instead of the named sample |
| `--bootstrap HOST:PORT` | `localhost:9092` | Kafka broker address |
| `--topic NAME` | `tasks.events` | Kafka topic to produce to |

---

## 7. Manual API Testing

### Endpoints

| Method | Path | Rate limit | Description |
|--------|------|------------|-------------|
| `GET` | `/tasks/{id}` | 120/min | Fetch a single task |
| `GET` | `/tasks/{id}/chain` | 60/min | Fetch the full ancestor chain |
| `GET` | `/health` | 30/min | Readiness probe — pings MongoDB, Redis, and Kafka consumer |
| `GET` | `/health/live` | — | Liveness probe — always 200 if the process is responding |

Requests exceeding the rate limit receive `429 Too Many Requests`.

Task IDs must match `^[a-zA-Z0-9_-]+$` (letters, digits, `_`, `-`). Other characters return `422 Unprocessable Entity`.

### Look up a single task

```
GET /tasks/t1
```

Expected response:
```json
{
  "id": "t1",
  "title": "Task 1",
  "parent_task_id": null
}
```

### Get the full dependency chain

```
GET /tasks/t3/chain
```

Expected response (chain from t3 up to root):
```json
[
  {"id": "t2", "title": "Task 2", "parent_task_id": "t1"},
  {"id": "t1", "title": "Task 1", "parent_task_id": null}
]
```

The response includes three diagnostic headers:

| Header | Values | Meaning |
|--------|--------|---------|
| `X-Chain-Complete` | `true` / `false` | `true` = chain reached a root with no parent; `false` = a parent is missing, the depth limit was reached, or a cycle was detected |
| `X-Chain-Depth-Limited` | `true` / `false` | `true` = chain was cut at the configured depth cap (default: 100, set via `MAX_CHAIN_DEPTH`); retrying will not yield more ancestors |
| `X-Chain-Cycle` | `true` / `false` | `true` = a circular dependency was detected in the data; `false` only guarantees no cycle *within the traversed depth* |

### Root task returns empty chain

```
GET /tasks/t1/chain   →   []   (X-Chain-Complete: true)
```

### Non-existent task returns 404

```
GET /tasks/no-such-task/chain   →   404 {"detail": "Task not found"}
```

### Request tracing

Pass `X-Request-ID: <your-trace-id>` on any request and the same value is echoed back in the response header. If omitted, a 12-character ID is generated automatically. Useful for correlating logs with upstream traces.

---

## 8. Acceptance Criteria — Step-by-Step Demo

A Postman collection (`task-dependency-service.postman_collection.json`) covers all acceptance criteria. Import it into Postman, set `baseUrl` to `http://localhost:8000`, and follow the steps below.

### AC1 — Kafka-ingested task can be looked up

```bash
python scripts/produce_tasks.py
```

Then: `GET /tasks/t1` → 200 with task data.

---

### AC2 — Full ordered dependency chain

After seeding:

```
GET /tasks/t3/chain   →   [t2, t1]
GET /tasks/t2/chain   →   [t1]
```

---

### AC3 — Root task returns empty chain

```
GET /tasks/t1/chain   →   []
```

---

### AC4 — Non-existent task returns 404

```
GET /tasks/no-such-task         →   404
GET /tasks/no-such-task/chain   →   404
```

---

### AC5 — Out-of-order arrival

Demonstrates that the chain resolves correctly even when a child task arrives before its parent.

**Step 1** — Send the child (parent does not exist yet):

```bash
python -c "
import asyncio, json
from aiokafka import AIOKafkaProducer
async def run():
    p = AIOKafkaProducer(bootstrap_servers='localhost:9092')
    await p.start()
    await p.send_and_wait('tasks.events', json.dumps({'id':'t-child','title':'Child','parent_task_id':'t-parent-late'}).encode())
    await p.stop()
asyncio.run(run())"
```

**Step 2** — Query the chain (partial — parent not yet ingested):

```
GET /tasks/t-child/chain   →   200  []
X-Chain-Complete: false
X-Chain-Depth-Limited: false
X-Chain-Cycle: false
```

**Step 3** — Send the parent:

```bash
python -c "
import asyncio, json
from aiokafka import AIOKafkaProducer
async def run():
    p = AIOKafkaProducer(bootstrap_servers='localhost:9092')
    await p.start()
    await p.send_and_wait('tasks.events', json.dumps({'id':'t-parent-late','title':'Parent','parent_task_id':None}).encode())
    await p.stop()
asyncio.run(run())"
```

**Step 4** — Query the chain again (now complete):

> **Note:** The partial chain from Step 2 is cached for up to 30 seconds (`CACHE_INCOMPLETE_CHAIN_TTL_SECONDS`). If you query immediately after Step 3, you may still receive the incomplete response while the cache entry is live. Wait ~30 seconds or add a small delay between steps 3 and 4.

```
GET /tasks/t-child/chain   →   200  [t-parent-late]
X-Chain-Complete: true
X-Chain-Depth-Limited: false
X-Chain-Cycle: false
```

---

### AC6 — Duplicate events do not corrupt data

Run the producer twice:

```bash
python scripts/produce_tasks.py
python scripts/produce_tasks.py
```

Then verify the task is not duplicated:

```
GET /tasks/t1   →   exactly one task, data unchanged
GET /tasks/t3/chain   →   still [t2, t1]
```

---

### AC7 — Performance (Redis caching)

Hit the same chain endpoint twice:

```
GET /tasks/t3/chain   →   200   (cache miss — computed from MongoDB)
GET /tasks/t3/chain   →   200   (cache hit — served from Redis)
```

Both responses return the same result. App logs show `Cache miss` then `Cache hit`.

---

### AC8 — Graceful degradation

**Redis down — falls back to MongoDB:**

```bash
docker compose stop redis
```

```
GET /tasks/t3/chain   →   200 with correct chain (no Redis, reads from MongoDB)
```

```bash
docker compose start redis
```

**MongoDB down — returns 503:**

```bash
docker compose stop mongo
```

```
GET /tasks/t1   →   503 {"detail": "Service temporarily unavailable"}
```

```bash
docker compose start mongo
```

---

## Production Deployment

### Health probes — liveness vs readiness

The service exposes two health endpoints with distinct responsibilities:

| Endpoint | Purpose | Probe type |
|----------|---------|------------|
| `GET /health/live` | Process-only — always 200 if the app responds | **Liveness** |
| `GET /health` | Pings MongoDB, Redis, and Kafka consumer | **Readiness** |

Wire them separately in your orchestrator. Using `/health` for liveness means a temporary MongoDB outage restarts all pods — the opposite of what you want during an incident.

**Docker / systemd restart policy:** use `/health/live`.  
**Load balancer drain check:** use `/health`.

---

### Graceful shutdown timing

The app drains the in-flight Kafka message on `SIGTERM` and waits up to `SHUTDOWN_TIMEOUT_SECONDS` (default 30) for the consumer to stop. Set your process manager's kill timeout to at least `SHUTDOWN_TIMEOUT_SECONDS + 5` seconds to avoid a `SIGKILL` interrupting a MongoDB write.

```
# Example: if SHUTDOWN_TIMEOUT_SECONDS=30, set:
terminationGracePeriodSeconds: 35   # Kubernetes pod spec
stop_timeout: 35                    # Docker Compose
```

---

### Security checklist

#### API key (required for external deployments)

By default all task endpoints are **publicly accessible**. The startup log will warn:

```
WARNING  API_KEY is unset — all endpoints are publicly accessible
```

Set a strong random key:

```bash
# Generate
python -c "import secrets; print(secrets.token_urlsafe(32))"
# Set
export API_KEY=<generated-value>
```

Clients send it as `X-API-Key: <value>`. `/health` and `/health/live` are intentionally unauthenticated for infrastructure probes.

---

#### TLS for backing services

All connections default to plaintext. Enable TLS by changing the connection URLs — no code change required.

**MongoDB:**
```
MONGODB_URL=mongodb://user:password@host:27017/taskdb?authSource=admin&tls=true
# Custom CA:
MONGODB_URL=mongodb://user:password@host:27017/taskdb?authSource=admin&tls=true&tlsCAFile=/etc/ssl/mongo-ca.pem
```

**Redis:**
```
# rediss:// (double-s) enables TLS automatically
REDIS_URL=rediss://:password@host:6379
```

**Kafka:**
Kafka TLS is not yet configurable via environment variables. To enable it, add `security_protocol="SSL"` and an `ssl_context` to `AIOKafkaConsumer` in `app/services/kafka_consumer.py`. Until then, ensure Kafka is on a private network segment.

---

#### Authentication for backing services

The `.env.example` and `docker-compose.yml` ship without credentials — suitable for local development only.

**MongoDB:**
```yaml
# docker-compose.yml
mongo:
  environment:
    MONGO_INITDB_ROOT_USERNAME: admin
    MONGO_INITDB_ROOT_PASSWORD: changeme
```
```
MONGODB_URL=mongodb://admin:changeme@localhost:27017/taskdb?authSource=admin
```

**Redis:**
```yaml
# docker-compose.yml
redis:
  command: redis-server --requirepass changeme
```
```
REDIS_URL=redis://:changeme@localhost:6379
```

---

### Rate limiting — multi-replica deployments

The default `RATE_LIMIT_STORAGE_URI=memory://` stores counters in process memory. With multiple replicas each process enforces limits independently, so a client hitting N replicas gets N× the documented rate. The startup log warns when this default is active:

```
WARNING  RATE_LIMIT_STORAGE_URI=memory:// — rate-limit counters are per-process ...
```

Point all replicas at the same Redis instance for cluster-wide enforcement:

```
RATE_LIMIT_STORAGE_URI=redis://localhost:6379
```

This can be the same Redis instance used for caching — it uses a separate key namespace.

---

### Environment variable reference

| Variable | Default | Description |
|----------|---------|-------------|
| `MONGODB_URL` | `mongodb://localhost:27017` | MongoDB connection string (supports `?tls=true`, auth) |
| `REDIS_URL` | `redis://localhost:6379` | Redis connection string (`rediss://` for TLS) |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker address |
| `KAFKA_TOPIC` | `tasks.events` | Topic to consume from |
| `KAFKA_CONSUMER_GROUP` | `task-dependency-service` | Consumer group ID |
| `API_KEY` | *(unset — public)* | Shared secret for `X-API-Key` header authentication |
| `LOG_FORMAT` | `text` | `json` for structured log ingestion (ELK, CloudWatch, Datadog) |
| `LOG_LEVEL` | `INFO` | Python log level |
| `RATE_LIMIT_STORAGE_URI` | `memory://` | `redis://host:port` for cluster-wide rate limiting |
| `MAX_CHAIN_DEPTH` | `100` | Maximum ancestor hops (1–1000); chains beyond this return `X-Chain-Depth-Limited: true` |
| `CACHE_CHAIN_TTL_SECONDS` | `3600` | Redis TTL for complete chains |
| `CACHE_INCOMPLETE_CHAIN_TTL_SECONDS` | `30` | Redis TTL for incomplete/partial chains |
| `SHUTDOWN_TIMEOUT_SECONDS` | `30` | Seconds to wait for the Kafka consumer to drain on shutdown |

---

## Connection Details

| Service | URL | Default Port |
|---------|-----|--------------|
| Kafka | localhost | 9092 |
| MongoDB | mongodb://localhost | 27017 |
| Redis | redis://localhost | 6379 |

Database: `taskdb`, Collection: `tasks`

---

## Known Setup Notes

### Kafka image

`bitnami/kafka:3.7` requires a paid Docker Hub subscription. Work around it by pulling the free Apache image and retagging it so Docker Compose finds the name it expects:

```bash
docker pull apache/kafka:3.7.0
docker tag apache/kafka:3.7.0 bitnami/kafka:3.7
```

`docker-compose.override.yml` is included in the repo. Docker Compose reads it automatically alongside `docker-compose.yml`, so no extra flags are needed — it replaces the Bitnami-specific environment variables with the equivalent Apache Kafka ones.

### Configuration

If your local ports differ from the docker-compose defaults (MongoDB 27017, Redis 6379, Kafka 9092), create a `.env` file. `.env.example` shows the format:

```bash
cp .env.example .env   # then edit the values to match your setup
```

Key tuneable settings:

| Variable | Default | Description |
|----------|---------|-------------|
| `MONGODB_URL` | `mongodb://localhost:27017` | MongoDB connection string |
| `REDIS_URL` | `redis://localhost:6379` | Redis connection string |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker address |
| `MAX_CHAIN_DEPTH` | `100` | Maximum ancestor hops per `$graphLookup` query (1–1000). Raise this for unusually deep task hierarchies; chains longer than this cap return `X-Chain-Depth-Limited: true`. |
| `CACHE_CHAIN_TTL_SECONDS` | `3600` | Redis TTL for complete chains (seconds) |
| `CACHE_INCOMPLETE_CHAIN_TTL_SECONDS` | `30` | Redis TTL for incomplete chains (seconds) |
| `API_KEY` | *(unset)* | When set, all endpoints require `X-API-Key: <value>` |
| `LOG_FORMAT` | `text` | Set to `json` in production for structured log ingestion |