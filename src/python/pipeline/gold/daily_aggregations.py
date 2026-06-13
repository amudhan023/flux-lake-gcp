"""Gold layer: daily merchant aggregations and hourly transaction volumes.

Two execution modes controlled by environment variables:
  ENABLE_DELTA_OPTIMIZE=false  → slow baseline (full table scan, no compaction)
  ENABLE_DELTA_OPTIMIZE=true   → optimized (partition pruning, OPTIMIZE first, broadcast joins)
"""
import os
from concurrent.futures import ThreadPoolExecutor
from datetime import date, timedelta
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

from ...utils.delta_utils import table_path, merge_into_delta, table_exists
from ...utils.logging_config import get_logger
from ...optimization.delta_optimizer import optimize_table
from ...metrics.prometheus_client import pipeline_records_processed
from ...tracing.otel_setup import get_tracer

OPTIMIZED = os.getenv("ENABLE_DELTA_OPTIMIZE", "true").lower() == "true"
INCREMENTAL = os.getenv("ENABLE_INCREMENTAL_GOLD", "true").lower() == "true"


def _get_report_date() -> str:
    return str(date.today() - timedelta(days=1))


def run_merchant_daily_summary(
    spark: SparkSession, run_id: str, report_date: str
) -> int:
    silver_path = table_path("silver", "cleansed_transactions")
    gold_path = table_path("gold", "daily_merchant_summary")
    merchant_path = table_path("bronze", "merchant_reference")

    if not table_exists(spark, silver_path):
        return 0

    silver_df = spark.read.format("delta").load(silver_path)

    if OPTIMIZED:
        silver_df = silver_df.filter(F.to_date("event_ts") == report_date)
    # else: full scan (baseline)

    daily = (
        silver_df.groupBy("merchant_id", F.to_date("event_ts").alias("report_date"))
        .agg(
            F.count("transaction_id").alias("transaction_count"),
            F.sum("amount_decimal").alias("total_amount"),
            F.avg("amount_decimal").alias("avg_amount"),
            F.countDistinct("customer_id").alias("unique_customers"),
        )
    )

    if OPTIMIZED and table_exists(spark, merchant_path):
        merchant_ref = spark.read.format("delta").load(merchant_path)
        if OPTIMIZED:
            # broadcast join — merchant_ref is small
            daily = daily.join(F.broadcast(merchant_ref), on="merchant_id", how="left")

    cnt = daily.count()
    merge_into_delta(
        spark, daily, gold_path,
        merge_key="merchant_id",
        update_columns=["transaction_count", "total_amount", "avg_amount", "unique_customers", "report_date"]
    )
    pipeline_records_processed.labels(stage="gold", table="daily_merchant_summary").inc(cnt)
    return cnt


def run_hourly_transaction_volume(
    spark: SparkSession, run_id: str, report_date: str
) -> int:
    silver_path = table_path("silver", "cleansed_transactions")
    gold_path = table_path("gold", "hourly_transaction_volume")

    if not table_exists(spark, silver_path):
        return 0

    silver_df = spark.read.format("delta").load(silver_path)
    if OPTIMIZED:
        silver_df = silver_df.filter(F.to_date("event_ts") == report_date)

    hourly = (
        silver_df
        .withColumn("hour", F.date_trunc("hour", "event_ts"))
        .groupBy("hour", "currency", "region")
        .agg(
            F.count("transaction_id").alias("transaction_count"),
            F.sum("amount_decimal").alias("total_volume"),
        )
    )

    cnt = hourly.count()
    hourly.write.format("delta").mode("append").partitionBy("hour").option(
        "mergeSchema", "true"
    ).save(gold_path)
    pipeline_records_processed.labels(stage="gold", table="hourly_transaction_volume").inc(cnt)
    return cnt


def run_gold_aggregations(
    spark: SparkSession, run_id: str = "manual", report_date: str | None = None
) -> dict:
    logger = get_logger("gold_aggregation", run_id)
    tracer = get_tracer()
    rd = report_date or _get_report_date()

    with tracer.start_as_current_span("gold_aggregation") as span:
        span.set_attribute("report_date", rd)
        span.set_attribute("optimized", OPTIMIZED)

        if OPTIMIZED:
            with tracer.start_as_current_span("delta_optimize"):
                optimize_table(spark, "silver", "cleansed_transactions", ["merchant_id", "customer_id"], run_id)

        with tracer.start_as_current_span("merchant_daily_summary"):
            merchant_cnt = run_merchant_daily_summary(spark, run_id, rd)

        with tracer.start_as_current_span("hourly_transaction_volume"):
            hourly_cnt = run_hourly_transaction_volume(spark, run_id, rd)

    logger.info(
        "Gold aggregations complete",
        extra={
            "records_count": merchant_cnt + hourly_cnt,
            "table": "gold",
            "duration_ms": 0,
        }
    )
    return {
        "merchant_summary_records": merchant_cnt,
        "hourly_volume_records": hourly_cnt,
        "report_date": rd,
        "optimized": OPTIMIZED,
    }
