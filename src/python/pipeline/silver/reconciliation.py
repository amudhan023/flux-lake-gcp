"""Silver reconciliation: join payments vs settlements, flag discrepancies."""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

from ...utils.delta_utils import table_path, merge_into_delta, table_exists
from ...utils.logging_config import get_logger
from ...metrics.prometheus_client import (
    pipeline_records_processed, dq_reconciliation_discrepancy
)
from ...tracing.otel_setup import get_tracer

TOLERANCE = 0.01


def run_reconciliation(spark: SparkSession, run_id: str = "manual", report_date: str | None = None) -> dict:
    logger = get_logger("reconciliation", run_id)
    tracer = get_tracer()

    payments_path = table_path("silver", "cleansed_transactions")
    settlements_path = table_path("bronze", "raw_settlements")
    ledger_path = table_path("silver", "reconciliation_ledger")

    if not table_exists(spark, payments_path):
        logger.warning("Silver transactions table missing, skipping reconciliation")
        return {"reconciled": 0, "discrepancies": 0}

    with tracer.start_as_current_span("silver_reconciliation") as span:
        payments = spark.read.format("delta").load(payments_path)
        settlements = (
            spark.read.format("delta").load(settlements_path)
            if table_exists(spark, settlements_path)
            else spark.createDataFrame([], payments.schema)
        )

        if report_date:
            payments = payments.filter(F.col("event_ts").cast("date") == report_date)

        # Aggregate settlements per merchant per day
        daily_settlements = (
            settlements
            .withColumn("settlement_date", F.to_date("timestamp"))
            .groupBy("merchant_id", "settlement_date")
            .agg(F.sum("gross_amount").alias("settled_amount"))
        )

        # Aggregate payments per merchant per day
        daily_payments = (
            payments
            .withColumn("payment_date", F.to_date("event_ts"))
            .groupBy("merchant_id", "payment_date")
            .agg(F.sum("amount_decimal").alias("payment_amount"))
        )

        with tracer.start_as_current_span("reconciliation_join"):
            joined = daily_payments.alias("p").join(
                daily_settlements.alias("s"),
                (F.col("p.merchant_id") == F.col("s.merchant_id"))
                & (F.col("p.payment_date") == F.col("s.settlement_date")),
                how="left",
            )

            ledger = joined.withColumn(
                "discrepancy_amount",
                F.abs(F.col("p.payment_amount") - F.coalesce(F.col("s.settled_amount"), F.lit(0)))
            ).withColumn(
                "reconciliation_status",
                F.when(F.col("s.settled_amount").isNull(), "awaiting_settlement")
                 .when(F.col("discrepancy_amount") == 0, "reconciled")
                 .when(F.col("discrepancy_amount") <= TOLERANCE, "reconciled_within_tolerance")
                 .otherwise("discrepancy_flagged")
            ).withColumn("reconciled_at", F.current_timestamp())

        disc_count = ledger.filter(F.col("reconciliation_status") == "discrepancy_flagged").count()
        awaiting_count = ledger.filter(F.col("reconciliation_status") == "awaiting_settlement").count()
        dq_reconciliation_discrepancy.labels(type="amount_mismatch").inc(disc_count)
        dq_reconciliation_discrepancy.labels(type="missing_settlement").inc(awaiting_count)

        merge_into_delta(
            spark, ledger, ledger_path,
            merge_key="merchant_id",
            update_columns=["reconciliation_status", "discrepancy_amount", "reconciled_at"]
        )

        total = ledger.count()
        pipeline_records_processed.labels(stage="silver", table="reconciliation_ledger").inc(total)
        logger.info(
            "Reconciliation complete",
            extra={"records_count": total, "table": "reconciliation_ledger"}
        )

    return {
        "reconciled": total - disc_count - awaiting_count,
        "discrepancies": disc_count,
        "awaiting_settlement": awaiting_count,
    }
