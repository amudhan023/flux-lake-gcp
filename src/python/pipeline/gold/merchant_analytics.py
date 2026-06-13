"""Gold: per-merchant analytics including dispute rate, refund ratio, settlement lag."""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

from ...utils.delta_utils import table_path, merge_into_delta, table_exists
from ...utils.logging_config import get_logger
from ...metrics.prometheus_client import pipeline_records_processed


def run_merchant_analytics(spark: SparkSession, run_id: str = "manual") -> dict:
    logger = get_logger("merchant_analytics", run_id)

    payments_path = table_path("silver", "cleansed_transactions")
    refunds_path = table_path("bronze", "raw_refunds")
    chargebacks_path = table_path("bronze", "raw_chargebacks")
    settlements_path = table_path("bronze", "raw_settlements")
    out_path = table_path("gold", "merchant_analytics")

    if not table_exists(spark, payments_path):
        return {"records": 0}

    payments = spark.read.format("delta").load(payments_path)
    payment_agg = (
        payments.groupBy("merchant_id")
        .agg(
            F.count("transaction_id").alias("total_transactions"),
            F.sum("amount_decimal").alias("total_gmv"),
        )
    )

    refunds = (
        spark.read.format("delta").load(refunds_path)
        if table_exists(spark, refunds_path)
        else spark.createDataFrame([], payments.schema)
    )
    refund_agg = (
        refunds.groupBy("merchant_id")
        .agg(F.count("refund_id").alias("refund_count"))
    )

    chargebacks = (
        spark.read.format("delta").load(chargebacks_path)
        if table_exists(spark, chargebacks_path)
        else spark.createDataFrame([], payments.schema)
    )
    cb_agg = (
        chargebacks.groupBy("merchant_id")
        .agg(F.count("dispute_id").alias("chargeback_count"))
    )

    result = (
        payment_agg
        .join(refund_agg, on="merchant_id", how="left")
        .join(cb_agg, on="merchant_id", how="left")
        .withColumn("refund_ratio", F.col("refund_count") / F.col("total_transactions"))
        .withColumn("chargeback_rate", F.col("chargeback_count") / F.col("total_transactions"))
        .fillna(0, subset=["refund_count", "chargeback_count", "refund_ratio", "chargeback_rate"])
        .withColumn("computed_at", F.current_timestamp())
    )

    cnt = result.count()
    merge_into_delta(spark, result, out_path, merge_key="merchant_id")
    pipeline_records_processed.labels(stage="gold", table="merchant_analytics").inc(cnt)
    logger.info("Merchant analytics complete", extra={"records_count": cnt})
    return {"records": cnt}
