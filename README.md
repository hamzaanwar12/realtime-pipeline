# realtime-pipeline

A small but production-shaped **streaming data pipeline**: clickstream events flow through **Redpanda** (Kafka API)
into **DuckDB** in micro-batches, and SQL models turn raw events into live metrics.

```
producer ──▶ Redpanda topic `clickstream` ──▶ consumer (micro-batch) ──▶ DuckDB raw_events ──▶ SQL models
 (fake events)      (keyed by user_id)          │ invalid → dead_letter                     events_per_minute
                                                └ commit offsets after DB commit            funnel, top_customers
```

## Design choices
- **Effectively-once delivery:** offsets are committed only after the DuckDB transaction commits
  (at-least-once), and `event_id` is a primary key with `insert or ignore`, so redelivered events are deduped.
- **Schema enforcement at the edge:** every payload is type-checked; anything malformed lands in `dead_letter`
  with the raw payload instead of crashing the consumer.
- **Idempotent producer** (`enable.idempotence`) and per-user keys keep each user's events ordered.
- **Models as SQL files:** `models/NN_name.sql` become views, rebuilt in order — dbt-style, without the dependency.

## Run

```bash
docker compose up --build            # Redpanda + producer + consumer
# query the warehouse after it fills
docker compose run --rm --entrypoint /app/.venv/bin/python consumer -c \
  "import duckdb; print(duckdb.connect('/data/warehouse.duckdb', read_only=True).sql('from funnel'))"
```

Local (Redpanda from compose on `localhost:19092`):

```bash
uv sync
uv run python -m pipeline.run produce &
uv run python -m pipeline.run consume
```

## Test

```bash
uv run pytest -q    # parsing, dead-lettering, redelivery dedupe, model outputs — no Kafka needed
```

## Stack
Python · Redpanda/Kafka (confluent-kafka) · DuckDB · SQL · Docker Compose · GitHub Actions
