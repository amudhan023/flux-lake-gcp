"""Silver layer: cleanse, validate, and deduplicate Bronze payments."""
import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

from ...utils.delta_utils import table_path, merge_into_delta, table_exists
from ...utils.logging_config import get_logger
from ...metrics.prometheus_client import pipeline_records_processed, dq_null_rate, dq_duplicate_count
from ...tracing.otel_setup import get_tracer
from ...ingestion.schemas import VALID_CURRENCIES


def _cast_and_validate(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    casted = (
        df.withColumn("amount_decimal", F.col("amount").cast(DecimalType(18, 2)))
          .withColumn("event_ts", F.to_timestamp("timestamp"))
          .withColumn("currency_valid", F.col("currency").isin(list(VALID_CURRENCIES)))
    )

    valid = casted.filter(
        F.col("amount_decimal").isNotNull()
        & (F.col("amount_decimal") > 0)
        & F.col("currency_valid")
        & F.col("event_ts").isNotNull()
        & F.col("merchant_id").isNotNull()
        & F.col("customer_id").isNotNull()
    )

    quarantined = casted.subtract(valid).withColumn("quarantine_reason",
        F.when(F.col("amount_decimal").isNull(), "null_amount")
         .when(F.col("amount_decimal") <= 0, "non_positive_amount")
         .when(~F.col("currency_valid"), "invalid_currency")
         .when(F.col("event_ts").isNull(), "invalid_timestamp")
         .when(F.col("merchant_id").isNull(), "null_merchant_id")
         .otherwise("unknown")
    )
    return valid, quarantined


def run_silver_cleanse(spark: SparkSession, run_id: str = "manual", report_date: str | None = None) -> dict:
    logger = get_logger("silver_cleanse", run_id)
    tracer = get_tracer()

    bronze_path = table_path("bronze", "raw_payments")
    silver_path = table_path("silver", "cleansed_transactions")
    quarantine_path = table_path("silver", "quarantine_payments")

    if not table_exists(spark, bronze_path):
        logger.warning("Bronze table not found, skipping silver cleanse")
        return {"records_processed": 0}

    with tracer.start_as_current_span("silver_cleanse") as span:
        bronze_df = spark.read.format("delta").load(bronze_path)

        if report_date:
            bronze_df = bronze_df.filter(F.col("event_ts").cast("date") == report_date)

        span.set_attribute("source_count", bronze_df.count())

        valid_df, quarantined_df = _cast_and_validate(bronze_df)

        # Emit null rate metrics for key columns
        total = valid_df.count()
        for col in ["merchant_id", "customer_id", "amount_decimal"]:
            null_count = valid_df.filter(F.col(col).isNull()).count()
            dq_null_rate.labels(table="cleansed_transactions", column=col).set(
                null_count / total if total > 0 else 0
            )

        # Deduplication
        pre_dedup_count = total
        valid_df = valid_df.dropDuplicates(["transaction_id"])
        post_dedup_count = valid_df.count()
        dupes = pre_dedup_count - post_dedup_count
        dq_duplicate_count.labels(table="cleansed_transactions").set(dupes)

        with tracer.start_as_current_span("dedup_merge"):
            merge_into_delta(
                spark, valid_df, silver_path, merge_key="transaction_id"
            )

        if not quarantined_df.rdd.isEmpty():
            quarantined_df.write.format("delta").mode("append").save(quarantine_path)

        pipeline_records_processed.labels(stage="silver", table="cleansed_transactions").inc(post_dedup_count)
        logger.info(
            "Silver cleanse complete",
            extra={"records_count": post_dedup_count, "table": "cleansed_transactions"}
        )

    return {
        "records_processed": post_dedup_count,
        "duplicates_removed": dupes,
        "quarantined": quarantined_df.count() if not quarantined_df.rdd.isEmpty() else 0,
    }
