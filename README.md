# Real-Time Data Streaming Pipeline

Local Kafka → PySpark Structured Streaming → Bronze / Silver / Gold lakehouse, queryable with DuckDB.

**Stack:** Python 3.12 · Apache Kafka (Docker, KRaft) · PySpark 3.5 · Delta Lake · Parquet · DuckDB

---

## Architecture

```
Python Producer                  Python Consumer (optional)
       │                                │
       ▼                                ▼
┌──────────────┐                 validate + DLQ topic
│ Kafka topic  │────────────────────────┘
│ transactions │  (3 partitions)
└──────┬───────┘
       │
       ▼
PySpark Structured Streaming (foreachBatch + checkpoint)
       │
       ├─► Bronze   raw Kafka payloads + metadata   → Parquet
       ├─► Quarantine  invalid rows                 → Parquet
       ├─► Silver   cleaned / typed / deduped       → Parquet + Delta
       └─► Gold     1-min window metrics + snapshot → Parquet + Delta
                                                      │
                                                      ▼
                                                   DuckDB SQL
```

| Layer | Path | Contents |
|-------|------|----------|
| Bronze | `data/lake/bronze/transactions` | Raw JSON fields + Kafka offset/partition |
| Quarantine | `data/lake/quarantine/transactions` | Failed validation |
| Silver | `data/lake/silver/transactions` | Clean events (also `data/lake/delta/silver_transactions`) |
| Gold | `data/lake/gold/metrics` | Per-customer 1-minute windows (also Delta) |
| Snapshot | `data/lake/gold/snapshot` | Latest batch totals |
| Checkpoints | `checkpoints/medallion` | Spark fault-tolerance state |

---

## Prerequisites

1. **Docker Desktop** running
2. **Homebrew OpenJDK 17** (required for Spark 3.5)
3. **Python 3.12** — macOS system Python 3.14 is too new for PySpark

```bash
brew install python@3.12 openjdk@17
```

---

## Configuration

Kafka settings live in `.env` / `.env.local` (see `.env.example`):

| Variable | Default |
|----------|---------|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` |
| `KAFKA_TOPIC` | `transactions` |
| `KAFKA_DLQ_TOPIC` | `transactions.dlq` |
| `KAFKA_CONSUMER_GROUP` | `transaction-validators` |

Python loads these automatically via `config/settings.py`. Spark still needs `source .env.local` for `JAVA_HOME`.

---

## Quick start

Open **4 terminals** from the repo root.

### 1) One-time setup

```bash
chmod +x scripts/*.sh
./scripts/setup.sh
source .venv/bin/activate
source .env.local
java -version   # should show 17.x
```

### 2) Start Kafka

```bash
./scripts/start_kafka.sh
```

### 3) Start the streaming job

```bash
source .venv/bin/activate && source .env.local
python -m streaming.medallion_pipeline --starting-offsets earliest
```

First launch downloads Spark Kafka/Delta jars (1–2 minutes). Watch for `[batch N] silver_rows=...` in the logs.

### 4) Produce events

```bash
source .venv/bin/activate
python -m producer.produce_transactions --rate 3 --duplicates --invalids --late
```

Stop with `Ctrl+C`, or cap output with `--max-events 100`.

### 5) (Optional) Run the Python consumer

```bash
source .venv/bin/activate
python -m consumer.consume_transactions --from-beginning
```

### 6) Query results

After ~20–30 seconds of events:

```bash
source .venv/bin/activate
python -m sql.query_gold
```

### Reset & stop

```bash
./scripts/reset_lake.sh
./scripts/stop_kafka.sh
```

To reprocess from the beginning: reset the lake, then restart streaming with `--starting-offsets earliest`. To resume after a restart, keep checkpoints and restart without `--reset`.

---

## Components

### Producer (`producer/produce_transactions.py`)

Publishes JSON transaction events to Kafka. Flags: `--duplicates`, `--invalids`, `--late`, `--rate`, `--max-events`.

### Consumer (`consumer/consume_transactions.py`)

Validates events and forwards failures to the DLQ topic. Runs independently of the Spark job.

### Streaming job (`streaming/medallion_pipeline.py`)

Per micro-batch (default every 10s):

1. Parse Kafka JSON
2. Write Bronze (partitioned by `event_date`)
3. Validate and quarantine failures
4. Silver: type casts, dedup, late-data filter (>10 min)
5. Gold: 1-minute window metrics + snapshot

Outputs Parquet and Delta under `data/lake/`.

---

## Concepts

### Kafka

| Concept | Role in this project |
|--------|----------------------|
| **Broker** | Kafka server (`streaming-kafka` container) |
| **Topic** | Named stream (`transactions`) |
| **Partition** | Ordered log slice (3 partitions) |
| **Offset** | Position of a record in a partition |
| **Producer** | Python event generator |
| **Consumer** | Python validator or Spark |
| **Consumer group** | Consumers sharing partition work |

### Batch vs streaming

| | Batch | Streaming |
|--|-------|-----------|
| Input | Bounded files / tables | Unbounded event stream |
| Latency | Minutes–hours | Seconds |
| State | Usually stateless per run | Checkpoints, watermarks, windows |

### Checkpointing

Spark stores Kafka offsets in `checkpoints/medallion`. On restart, processing resumes from the last committed batch. Delete checkpoints only when you intend to re-read Kafka from scratch.

### Late-arriving data

The producer can emit old timestamps (`--late`). Silver drops events more than 10 minutes behind the newest event in the batch.

### Delivery semantics

- Kafka producer: idempotent (`enable.idempotence=True`)
- Spark → Parquet: at-least-once (retries may duplicate rows before checkpoint commit)
- Mitigations: Silver `dropDuplicates`, Delta tables

| Challenge | Handling |
|-----------|----------|
| Duplicates | Producer flag + Silver `dropDuplicates` |
| Failed messages | Consumer → DLQ; Spark → quarantine |
| Late data | 10-minute lag filter |
| Fault tolerance | Spark checkpoints |

---

## Project layout

```
├── config/                 # Env-based settings
├── consumer/
├── producer/
├── streaming/
├── sql/
├── scripts/
├── docker-compose.yml
├── requirements.txt
├── data/lake/              # Created at runtime
└── checkpoints/            # Created at runtime
```

---

## Troubleshooting

| Problem | Fix |
|---------|-----|
| `Docker is not running` | Start Docker Desktop |
| `Java Runtime` not found | `source .env.local` or `brew install openjdk@17` |
| PySpark errors on Python 3.14 | Use the project `.venv` (Python 3.12) |
| No data in lake | Start producer and streaming with `--starting-offsets earliest` |
| Kafka connection refused | Run `./scripts/start_kafka.sh` |
| Fresh re-run | `./scripts/reset_lake.sh`, then restart streaming |
