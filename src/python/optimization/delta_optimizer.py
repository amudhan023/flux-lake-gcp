"""Delta Lake optimization utilities: OPTIMIZE, ZORDER, VACUUM."""
import os
import time
from pyspark.sql import SparkSession

from ..utils.delta_utils import table_path
from ..utils.logging_config import get_logger
from ..metrics.prometheus_client import (
    delta_files_before_optimize,
    delta_files_after_optimize,
    delta_optimize_duration,
    delta_vacuum_files_deleted,
)


VACUUM_RETENTION_HOURS = int(os.getenv("DELTA_VACUUM_RETENTION_HOURS", "168"))
ENABLE_ZORDER = os.getenv("ENABLE_ZORDER", "true").lower() == "true"
ENABLE_OPTIMIZE = os.getenv("ENABLE_DELTA_OPTIMIZE", "true").lower() == "true"


def _file_count(spark: SparkSession, path: str) -> int:
    try:
        detail = spark.sql(f"DESCRIBE DETAIL delta.`{path}`").collect()[0]
        return detail["numFiles"]
    except Exception:
        return -1


def optimize_table(
    spark: SparkSession,
    layer: str,
    table: str,
    zorder_cols: list[str] | None = None,
    run_id: str = "manual",
) -> dict:
    logger = get_logger("delta_optimizer", run_id)

    if not ENABLE_OPTIMIZE:
        logger.info("OPTIMIZE disabled by config", extra={"table": table})
        return {"skipped": True}

    path = table_path(layer, table)
    label = f"{layer}.{table}"

    before = _file_count(spark, path)
    delta_files_before_optimize.labels(table=label).set(before)

    t0 = time.time()
    if ENABLE_ZORDER and zorder_cols:
        cols = ", ".join(zorder_cols)
        spark.sql(f"OPTIMIZE delta.`{path}` ZORDER BY ({cols})")
    else:
        spark.sql(f"OPTIMIZE delta.`{path}`")
    elapsed = time.time() - t0

    after = _file_count(spark, path)
    delta_files_after_optimize.labels(table=label).set(after)
    delta_optimize_duration.labels(table=label).observe(elapsed)

    logger.info(
        "OPTIMIZE complete",
        extra={
            "table": label,
            "files_compacted": max(0, before - after),
            "duration_ms": int(elapsed * 1000),
        }
    )
    return {"before": before, "after": after, "duration_s": elapsed}


def vacuum_table(spark: SparkSession, layer: str, table: str, run_id: str = "manual") -> dict:
    logger = get_logger("delta_optimizer", run_id)
    path = table_path(layer, table)

    if VACUUM_RETENTION_HOURS < 168:
        logger.warning("Retention hours below minimum 168h, clamping to 168h")
        retention = 168
    else:
        retention = VACUUM_RETENTION_HOURS

    result = spark.sql(f"VACUUM delta.`{path}` RETAIN {retention} HOURS DRY RUN")
    files_to_delete = result.count()

    spark.sql(f"VACUUM delta.`{path}` RETAIN {retention} HOURS")
    delta_vacuum_files_deleted.labels(table=f"{layer}.{table}").inc(files_to_delete)

    logger.info("VACUUM complete", extra={"table": f"{layer}.{table}", "files_compacted": files_to_delete})
    return {"files_deleted": files_to_delete, "retention_hours": retention}


def run_full_optimization(spark: SparkSession, run_id: str = "manual") -> dict:
    results = {}
    tables = [
        ("silver", "cleansed_transactions", ["merchant_id", "customer_id"]),
        ("silver", "reconciliation_ledger", ["merchant_id"]),
        ("gold", "daily_merchant_summary", ["merchant_id"]),
        ("gold", "hourly_transaction_volume", []),
        ("gold", "ml_feature_store", ["customer_id"]),
    ]
    for layer, table, zorder in tables:
        results[f"{layer}.{table}"] = optimize_table(spark, layer, table, zorder, run_id)
    return results
