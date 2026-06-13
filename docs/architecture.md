# Architecture

## Data Flow

```
Event Producers (Locust / kafka_producer.py)
        │
        │ JSON events → topic: payments.raw
        ▼
    Apache Kafka (12 partitions)
        │
        │ Spark Structured Streaming (micro-batch)
        ▼
┌─────────────────────────────────────────────────────┐
│               BRONZE LAYER (Delta Lake)              │
│  s3://data-lake/bronze/                              │
│  ├── raw_payments/  (partition: year/month/day/region)│
│  ├── raw_refunds/                                    │
│  ├── raw_chargebacks/                                │
│  ├── raw_settlements/                                │
│  └── dead_letter/   (schema-rejected events)         │
└─────────────────────────────────────────────────────┘
        │
        │ Batch job (nightly trigger)
        │ OPTIMIZE + Z-ORDER on merchant_id, customer_id
        ▼
┌─────────────────────────────────────────────────────┐
│               SILVER LAYER (Delta Lake)              │
│  s3://data-lake/silver/                              │
│  ├── cleansed_transactions/  (deduped, typed, validated)│
│  ├── reconciliation_ledger/  (payment ↔ settlement)  │
│  └── dispute_registry/                               │
└─────────────────────────────────────────────────────┘
        │
        │ Batch job (after Silver complete)
        │ Partition pruning + broadcast joins
        ▼
┌─────────────────────────────────────────────────────┐
│               GOLD LAYER (Delta Lake)                │
│  s3://data-lake/gold/                                │
│  ├── daily_merchant_summary/     (BI-ready)          │
│  ├── hourly_transaction_volume/  (real-time BI)      │
│  ├── reconciliation_report/      (discrepancy flags) │
│  ├── ml_feature_store_customers/ (rolling features)  │
│  └── ml_feature_store_merchants/ (rolling features)  │
└─────────────────────────────────────────────────────┘
```

## Design Decisions

**MinIO over LocalStack:** MinIO is purpose-built for object storage with full S3 API compatibility and significantly lower resource overhead. LocalStack requires more memory and is primarily aimed at multi-AWS-service emulation — overkill for S3-only workloads.

**Spark Standalone over Kubernetes (local dev):** Kubernetes adds significant setup overhead (kind/minikube, node pools, RBAC). Spark Standalone mode gives us a real multi-worker cluster with the exact same code path, running in Docker Compose with a single command. Production would use EKS/GKE Spark Operator, but the pipeline code is identical.

**Delta Lake over Parquet/Iceberg:** Delta provides ACID transactions, schema enforcement, time travel, and OPTIMIZE/Z-ORDER all as first-class operations with Python/Scala APIs. The reconciliation use case specifically requires atomic writes and time travel for audit trails.

**Exactly-once via MERGE:** Bronze layer uses `MERGE INTO` on `transaction_id` rather than INSERT. This makes every pipeline run idempotent — safe to rerun on failures without duplicating data.
