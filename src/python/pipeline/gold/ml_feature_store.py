"""Gold: ML feature store — customer and merchant rolling-window features."""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from ...utils.delta_utils import table_path, table_exists
from ...utils.logging_config import get_logger
from ...metrics.prometheus_client import pipeline_records_processed


def _rolling_customer_features(payments: DataFrame) -> DataFrame:
    w7 = Window.partitionBy("customer_id").orderBy(F.col("event_ts").cast("long")).rangeBetween(-7 * 86400, 0)
    w30 = Window.partitionBy("customer_id").orderBy(F.col("event_ts").cast("long")).rangeBetween(-30 * 86400, 0)
    w90 = Window.partitionBy("customer_id").orderBy(F.col("event_ts").cast("long")).rangeBetween(-90 * 86400, 0)

    return payments.select(
        "customer_id",
        F.sum("amount_decimal").over(w7).alias("spend_7d"),
        F.sum("amount_decimal").over(w30).alias("spend_30d"),
        F.sum("amount_decimal").over(w90).alias("spend_90d"),
        F.count("transaction_id").over(w7).alias("tx_count_7d"),
        F.count("transaction_id").over(w30).alias("tx_count_30d"),
        F.current_timestamp().alias("feature_ts"),
    ).dropDuplicates(["customer_id"])


def _rolling_merchant_features(payments: DataFrame) -> DataFrame:
    w30 = Window.partitionBy("merchant_id").orderBy(F.col("event_ts").cast("long")).rangeBetween(-30 * 86400, 0)

    return payments.select(
        "merchant_id",
        F.sum("amount_decimal").over(w30).alias("gmv_30d"),
        F.avg("amount_decimal").over(w30).alias("avg_ticket_30d"),
        F.count("transaction_id").over(w30).alias("tx_count_30d"),
        F.current_timestamp().alias("feature_ts"),
    ).dropDuplicates(["merchant_id"])


def run_feature_store(spark: SparkSession, run_id: str = "manual") -> dict:
    logger = get_logger("ml_feature_store", run_id)
    payments_path = table_path("silver", "cleansed_transactions")

    if not table_exists(spark, payments_path):
        return {"customer_features": 0, "merchant_features": 0}

    payments = spark.read.format("delta").load(payments_path).withColumn(
        "event_ts", F.to_timestamp("event_ts")
    )

    customer_features = _rolling_customer_features(payments)
    merchant_features = _rolling_merchant_features(payments)

    c_cnt = customer_features.count()
    m_cnt = merchant_features.count()

    customer_features.write.format("delta").mode("overwrite").partitionBy("feature_ts").option(
        "mergeSchema", "true"
    ).save(table_path("gold", "ml_feature_store_customers"))

    merchant_features.write.format("delta").mode("overwrite").option(
        "mergeSchema", "true"
    ).save(table_path("gold", "ml_feature_store_merchants"))

    pipeline_records_processed.labels(stage="gold", table="ml_feature_store").inc(c_cnt + m_cnt)
    logger.info("ML feature store updated", extra={"records_count": c_cnt + m_cnt})
    return {"customer_features": c_cnt, "merchant_features": m_cnt}
