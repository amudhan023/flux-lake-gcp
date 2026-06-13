# Observability

## Prometheus Metrics Reference

### Pipeline SLA
| Metric | Type | Labels | When it fires |
|--------|------|--------|---------------|
| `pipeline_run_duration_seconds` | Histogram | stage, pipeline_name | On each pipeline stage completion |
| `pipeline_records_processed_total` | Counter | stage, table | Per record written to Delta |
| `pipeline_last_success_timestamp` | Gauge | pipeline_name | On successful pipeline finish |
| `pipeline_sla_seconds` | Gauge | pipeline_name | Total pipeline wall-clock time |

### Delta Lake Health
| Metric | Labels | Meaning |
|--------|--------|---------|
| `delta_files_before_optimize` | table | File count snapshot before OPTIMIZE |
| `delta_files_after_optimize` | table | File count snapshot after OPTIMIZE |
| `delta_optimize_duration_seconds` | table | Time OPTIMIZE took |
| `delta_vacuum_files_deleted` | table | Files reclaimed by VACUUM |

### Data Quality
| Metric | Labels | Meaning |
|--------|--------|---------|
| `dq_null_rate` | table, column | Fraction of nulls in a key column |
| `dq_duplicate_count` | table | Records removed by deduplication |
| `dq_reconciliation_discrepancy_total` | type | Flagged discrepancies (amount_mismatch / missing_settlement) |

## Reading the Grafana Dashboards

### Pipeline Overview
- **SLA Trend**: `pipeline_sla_seconds` over 7 days — see if nightly runs are getting slower
- **Stage Breakdown**: bar chart of stage durations — identify which stage is the bottleneck
- **Run Success Rate**: `pipeline_last_success_timestamp` staleness — alert if >25h since last success

### Spark Performance
- **Shuffle bytes**: high values indicate missing broadcast join opportunities
- **Executor memory**: watch for spill (memory approaching max)
- **Task failures**: any increment signals data quality or resource issues

### Delta Lake Health
- **Files before/after OPTIMIZE**: large ratio (>10:1) confirms OPTIMIZE is effective
- **OPTIMIZE duration**: should be <10min for Silver tables up to 1TB

### Kafka Topics
- **Consumer lag**: during normal ops should return to 0 within 5 min of load burst
- **Messages/sec**: use to correlate Grafana spike → load test scenario

### SLA Comparison
- Populated by `make benchmark` — shows per-stage slow vs optimized bar chart
- Source: `benchmark_results.json` via Grafana JSON datasource

### Data Quality
- **Null rate per column**: should be 0 for `merchant_id`, `amount_decimal`
- **Reconciliation discrepancies**: fires during month-end scenario load test

## Correlating Across Grafana → Jaeger → Kibana

Every pipeline run has a `pipeline_run_id` (format: `run_YYYYMMDD_HHMMSS_xxxxxx`).

1. In **Grafana** — find a spike in `pipeline_run_duration_seconds`; note the timestamp.
2. In **Jaeger** — search for service `pipeline` with the timestamp range; find the root span `pipeline_run`. The `pipeline_run_id` appears as a span attribute.
3. In **Kibana** — filter by `pipeline_run_id` field. Each log line from every stage of that run appears in chronological order.
4. Click the `trace_id` link from Kibana → jumps directly to the Jaeger trace.

## Example Trace Walkthrough

```
pipeline_run (root, ~137m total)
  ├── silver_cleanse          (38m)
  │     ├── dedup_merge       — Delta MERGE on 50M records
  │     └── quarantine_write  — 0.01% of records go here
  ├── reconciliation          (20m)
  │     └── reconciliation_join — daily_payments LEFT JOIN daily_settlements
  └── gold_aggregation        (35m)
        ├── delta_optimize    — 842 files compacted
        ├── merchant_daily_summary  — broadcast join + GROUP BY
        └── hourly_volume     — GROUP BY hour, currency, region
```

Each span reports: `table_name`, `records_count`, `partition_date`, `spark_job_id`.
