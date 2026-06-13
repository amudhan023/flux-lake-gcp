# Runbook

## Reprocessing a Historical Date

Use Delta time travel to reset a date partition and rerun:

```bash
# 1. Inside pipeline-api container
docker exec -it pipeline-api bash

# 2. Launch pyspark shell
pyspark --master spark://spark-master:7077

# 3. Delete the Gold partition for the date to reprocess
from delta.tables import DeltaTable
dt = DeltaTable.forPath(spark, "s3a://data-lake/gold/daily_merchant_summary")
dt.delete("report_date = '2026-01-15'")

# 4. Trigger pipeline with specific date
import requests
requests.post("http://pipeline-api:8000/trigger", json={"report_date": "2026-01-15"})
```

## Vacuuming Old Delta Files

```bash
# Safe vacuum — keeps 7 days (168h) of history
docker exec pipeline-api python -c "
from src.python.utils.spark_session import get_spark_session
from src.python.optimization.delta_optimizer import vacuum_table
spark = get_spark_session()
for layer, table in [('silver','cleansed_transactions'), ('gold','daily_merchant_summary')]:
    result = vacuum_table(spark, layer, table)
    print(f'{layer}.{table}: {result[\"files_deleted\"]} files deleted')
"
```

## Resetting Kafka Consumer Offsets

```bash
# Reset to earliest (reprocess all events)
docker exec kafka kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --group pipeline-consumer \
  --topic payments.raw \
  --reset-offsets --to-earliest --execute

# Reset to specific timestamp
docker exec kafka kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --group pipeline-consumer \
  --topic payments.raw \
  --reset-offsets --to-datetime 2026-01-15T00:00:00.000 --execute
```

## Scaling Spark Workers

```bash
# Scale to 4 workers without restarting other services
SPARK_WORKERS=4 make up

# Or directly via docker compose
docker compose up -d --scale spark-worker-1=4
```

## Recovering from a Failed Pipeline Run Mid-Execution

1. **Identify the failed run** — check `/status` endpoint or Grafana for the stalled run_id.
2. **Check the failed stage** — `make logs | grep "ERROR\|WARN"`.
3. **Delta tables are safe** — Delta's ACID guarantees the table is consistent even after a mid-write crash. No partial data was committed.
4. **Re-trigger** — `make run-pipeline`. The pipeline is fully idempotent via MERGE.

If Silver is stuck mid-MERGE:
```bash
# List Delta transaction log to confirm last committed version
spark.sql("DESCRIBE HISTORY delta.`s3a://data-lake/silver/cleansed_transactions`").show(5)
```

The last `COMMIT` entry is the authoritative state. Anything after it was rolled back automatically.

## Common Issues

| Symptom | Likely Cause | Fix |
|---------|-------------|-----|
| `Connection refused: minio:9000` | MinIO not healthy yet | `make logs minio` — wait for "S3-API: http://..." |
| Spark OOM executor killed | `SPARK_EXECUTOR_MEMORY` too low | Set `SPARK_EXECUTOR_MEMORY=8g` in `.env`, `make down && make up` |
| Kafka `LEADER_NOT_AVAILABLE` | Kafka still initializing | Wait 30s; brokers elect leader within 15s |
| Delta `TransactionConflictException` | Two writers racing on same partition | Normal — Delta retries automatically. If persistent, reduce Spark workers. |
| Grafana shows no data | Pipeline hasn't run yet | Run `make seed && make run-pipeline` first |
