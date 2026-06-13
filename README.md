# Scalable Batch Data Processing Pipeline

**PySpark + Delta Lake + S3 batch pipeline achieving 60% SLA improvement (5h → <2h)**

> Financial transaction processing — simulating a payments/e-commerce platform with daily batch reconciliation, historical aggregations, and curated analytics.

---

## What This Demonstrates

- **Medallion architecture** — Bronze (raw landing) → Silver (cleansed/reconciled) → Gold (aggregated/ML-ready)
- **60% SLA improvement** — from ~5h baseline to <2h via Delta OPTIMIZE, Z-ORDER, partition pruning, broadcast joins, and AQE
- **Full observability** — Prometheus metrics, OpenTelemetry traces (Jaeger), structured logs (ELK), 6 Grafana dashboards
- **Exactly-once ingestion** — Delta MERGE deduplicates on `transaction_id`
- **Financial reconciliation** — payments vs settlements with discrepancy detection and tolerance bands
- **ML feature store** — 7/30/90-day rolling customer and merchant features

---

## Prerequisites

- Docker Desktop (16GB RAM recommended, 20GB disk)
- GNU Make
- Python 3.10+

---

## Quick Start

```bash
# 1. Start the full stack
make up

# 2. Seed 90 days of historical Bronze data (~450k records)
make seed

# 3. Run the full pipeline
make run-pipeline
```

---

## Service URLs

| Service        | URL                              | Credentials       |
|----------------|----------------------------------|-------------------|
| Spark Master   | http://localhost:8080            | —                 |
| MinIO Console  | http://localhost:9001            | minioadmin / minioadmin123 |
| Kafka UI       | http://localhost:8090            | —                 |
| Grafana        | http://localhost:3000            | admin / admin123  |
| Jaeger         | http://localhost:16686           | —                 |
| Kibana         | http://localhost:5601            | —                 |
| Pipeline API   | http://localhost:8000            | —                 |
| Prometheus     | http://localhost:9090            | —                 |

---

## Architecture

```
Kafka (payments.raw)
     │
     ▼
┌────────────┐   Delta MERGE    ┌─────────────────────────────┐
│   Bronze   │ ──────────────▶  │   s3://data-lake/bronze/    │
│  Ingestion │                  │   raw_payments (partitioned) │
└────────────┘                  └─────────────────────────────┘
                                           │
                                    OPTIMIZE + Z-ORDER
                                           │
                                           ▼
                                ┌─────────────────────────────┐
│   Silver   │ ──────────────▶  │ cleansed_transactions        │
│  Cleanse   │   dedup + cast   │ reconciliation_ledger        │
└────────────┘                  └─────────────────────────────┘
                                           │
                                   partition pruning
                                   broadcast joins
                                           │
                                           ▼
                                ┌─────────────────────────────┐
│    Gold    │ ──────────────▶  │ daily_merchant_summary       │
│ Aggregation│                  │ hourly_transaction_volume    │
└────────────┘                  │ ml_feature_store             │
                                └─────────────────────────────┘
```

---

## Running Tests

```bash
make test-unit          # ~30 seconds, no Docker required
make infra-up           # start infrastructure for integration tests
make test-integration   # ~5 minutes, real Spark + Delta Lake
make test-dq            # ~2 minutes, Great Expectations suite
make benchmark          # ~15 minutes, produces SLA comparison
make load-test          # ~60 minutes, all 4 Locust scenarios
```

---

## Load Test Scenarios

| Scenario | Events/min | Duration | Validates |
|----------|-----------|----------|-----------|
| Normal Daily | 10,000 | 60m | Steady-state pipeline |
| Peak Traffic | 10k→100k→ramp down | 30m | Backpressure handling |
| Batch Catchup | High burst | 30m | 30-day backfill |
| Month-End Reconciliation | 10,000 (5% bad settlements) | 60m | Discrepancy detection |

After starting: `make load-test` prints the Grafana URL with the Pipeline Overview dashboard.

---

## Configuration Reference

All tunable parameters live in `.env` (copy from `.env.example`):

| Variable | Default | Description |
|----------|---------|-------------|
| `SPARK_WORKERS` | `2` | Worker count (scale with `SPARK_WORKERS=8 make up`) |
| `SPARK_EXECUTOR_MEMORY` | `4g` | Memory per executor |
| `KAFKA_PARTITIONS` | `12` | Topic partition count |
| `ENABLE_DELTA_OPTIMIZE` | `true` | Run OPTIMIZE before Gold aggregation |
| `ENABLE_ZORDER` | `true` | Z-ORDER on merchant_id, customer_id |
| `ENABLE_AQE` | `true` | Adaptive Query Execution |
| `ENABLE_BROADCAST_JOINS` | `true` | Broadcast small dimension tables |
| `ENABLE_INCREMENTAL_GOLD` | `true` | Delta CDC incremental Gold processing |

---

## Troubleshooting

**Port already in use:** Check `lsof -i :<port>` and kill the conflicting process, or edit docker-compose.yml to remap.

**OOM / Spark executor killed:** Increase `SPARK_EXECUTOR_MEMORY` in `.env` and `make down && make up`.

**MinIO auth failures:** Ensure `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` match `MINIO_ROOT_USER`/`MINIO_ROOT_PASSWORD`.

**Kafka not ready:** `make logs` and wait for `kafka    | [KafkaServer] started`. Health checks usually handle this automatically.
