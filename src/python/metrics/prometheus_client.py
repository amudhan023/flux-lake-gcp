import os
from prometheus_client import Counter, Gauge, Histogram, start_http_server

_PORT = int(os.getenv("PIPELINE_API_PORT", "8000"))

# Pipeline SLA metrics
pipeline_run_duration = Histogram(
    "pipeline_run_duration_seconds",
    "Pipeline stage run duration in seconds",
    ["stage", "pipeline_name"],
    buckets=[30, 60, 120, 300, 600, 1200, 3600, 7200],
)

pipeline_records_processed = Counter(
    "pipeline_records_processed_total",
    "Total records processed per stage",
    ["stage", "table"],
)

pipeline_last_success = Gauge(
    "pipeline_last_success_timestamp",
    "Unix timestamp of last successful pipeline run",
    ["pipeline_name"],
)

pipeline_sla = Gauge(
    "pipeline_sla_seconds",
    "Total pipeline run time for SLA tracking",
    ["pipeline_name"],
)

# Delta Lake metrics
delta_files_before_optimize = Gauge(
    "delta_files_before_optimize",
    "File count in Delta table before OPTIMIZE",
    ["table"],
)

delta_files_after_optimize = Gauge(
    "delta_files_after_optimize",
    "File count in Delta table after OPTIMIZE",
    ["table"],
)

delta_optimize_duration = Histogram(
    "delta_optimize_duration_seconds",
    "Time taken to run Delta OPTIMIZE",
    ["table"],
)

delta_vacuum_files_deleted = Counter(
    "delta_vacuum_files_deleted",
    "Files deleted by Delta VACUUM",
    ["table"],
)

# Data Quality
dq_null_rate = Gauge(
    "dq_null_rate",
    "Null rate for a specific column in a table",
    ["table", "column"],
)

dq_duplicate_count = Gauge(
    "dq_duplicate_count",
    "Duplicate record count in a table",
    ["table"],
)

dq_reconciliation_discrepancy = Counter(
    "dq_reconciliation_discrepancy_total",
    "Reconciliation discrepancies found",
    ["type"],
)

# Spark metrics
spark_job_duration = Histogram(
    "spark_job_duration_seconds",
    "Duration of a Spark job",
    ["job_id", "stage"],
)

spark_shuffle_bytes = Counter(
    "spark_shuffle_bytes_written",
    "Bytes written during Spark shuffle",
    ["job_id"],
)

spark_executor_memory = Gauge(
    "spark_executor_memory_used_bytes",
    "Memory used by Spark executor",
    ["executor_id"],
)

spark_task_failures = Counter(
    "spark_task_failure_total",
    "Spark task failure count",
    ["stage"],
)

# Kafka
kafka_messages_consumed = Counter(
    "kafka_messages_consumed_total",
    "Total Kafka messages consumed",
    ["topic"],
)

kafka_consumer_lag = Gauge(
    "kafka_consumer_lag",
    "Kafka consumer lag",
    ["topic", "partition"],
)

# Schema errors / dead letter
schema_errors = Counter(
    "schema_errors_total",
    "Records rejected due to schema errors",
    ["table"],
)
