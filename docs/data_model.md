# Data Model

## Event Types (Bronze Landing Zone)

### payment_created (80% of traffic)
| Field | Type | Description |
|-------|------|-------------|
| transaction_id | STRING NOT NULL | Globally unique payment ID |
| merchant_id | STRING NOT NULL | Merchant identifier (merch_1..merch_500) |
| customer_id | STRING NOT NULL | Customer identifier (cust_1..cust_10000) |
| amount | DOUBLE NOT NULL | Transaction amount |
| currency | STRING | ISO 4217 code (USD, EUR, GBP, SGD) |
| gateway | STRING | Payment processor |
| region | STRING | Geographic region for partition key |
| timestamp | STRING | ISO 8601 event time |

### refund_initiated (10%)
| Field | Type | Description |
|-------|------|-------------|
| refund_id | STRING NOT NULL | Unique refund ID |
| original_tx_id | STRING | References payment_created.transaction_id |
| refund_amount | DOUBLE | Partial or full refund |
| reason_code | STRING | customer_request / fraud / duplicate |

### chargeback_filed (2%)
| Field | Type | Description |
|-------|------|-------------|
| dispute_id | STRING NOT NULL | Unique dispute ID |
| original_tx_id | STRING | References payment_created.transaction_id |
| dispute_amount | DOUBLE | Full amount disputed |
| status | STRING | filed / under_review / resolved_* |

### settlement_processed (8%)
| Field | Type | Description |
|-------|------|-------------|
| batch_id | STRING NOT NULL | Settlement batch identifier |
| merchant_id | STRING NOT NULL | Merchant receiving funds |
| gross_amount | DOUBLE | Pre-fee settlement total |
| fees | DOUBLE | Gateway/processing fees |
| net_amount | DOUBLE | gross_amount - fees |

## Silver Layer Schema

### cleansed_transactions
All payment_created fields, plus:
- `amount_decimal`: DECIMAL(18,2) — cast and validated
- `event_ts`: TIMESTAMP — parsed from string timestamp
- Partitioned by: `year/month/day`

### reconciliation_ledger
| Field | Type |
|-------|------|
| merchant_id | STRING |
| payment_date | DATE |
| payment_amount | DECIMAL(18,2) |
| settled_amount | DECIMAL(18,2) nullable |
| discrepancy_amount | DECIMAL(18,2) |
| reconciliation_status | STRING |
| reconciled_at | TIMESTAMP |

Status values: `reconciled`, `reconciled_within_tolerance`, `discrepancy_flagged`, `awaiting_settlement`

## Gold Layer Schema

### daily_merchant_summary
| Field | Type |
|-------|------|
| merchant_id | STRING |
| report_date | DATE |
| transaction_count | LONG |
| total_amount | DECIMAL(18,2) |
| avg_amount | DECIMAL(18,2) |
| unique_customers | LONG |

### hourly_transaction_volume
| Field | Type |
|-------|------|
| hour | TIMESTAMP |
| currency | STRING |
| region | STRING |
| transaction_count | LONG |
| total_volume | DECIMAL(18,2) |

## Partition Strategy

| Layer | Partition Keys | Rationale |
|-------|----------------|-----------|
| Bronze | year/month/day/region | Regional partition supports geo-filtered queries |
| Silver | year/month/day | Date-aligned with SLA window (process yesterday's data) |
| Gold | report_date | Single-column partition — queries always filter by exact date |

## Delta Lake Features

| Feature | Where Used | Why |
|---------|-----------|-----|
| ACID transactions | All writes | Concurrent Spark workers can't corrupt the table |
| Schema enforcement | Bronze ingest | Rejects malformed events at write time |
| Schema evolution | Silver → nullable columns | Add ML feature columns without full rewrites |
| Time travel | Reconciliation audit | `VERSION AS OF` lets us reprocess any historical date |
| Z-ORDER | Silver merchant_id/customer_id | 60–80% data skip on merchant-scoped Gold queries |
| OPTIMIZE | Before Gold aggregation | Eliminates small-file overhead from streaming micro-batches |
| VACUUM | Scheduled | Reclaims storage from superseded Delta versions |
| Change Data Feed | Silver → Gold | Only recompute Gold partitions where Silver changed |
