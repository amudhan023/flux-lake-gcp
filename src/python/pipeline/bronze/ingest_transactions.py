"""Bronze layer: Kafka → Delta Lake.

Reads from Kafka topic `payments.raw` and writes raw events to Delta Bronze tables
partitioned by year/month/day/region. Schema-invalid rows go to the dead_letter table.
"""
import os
import json
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from ...utils.spark_session import get_spark_session
from ...utils.delta_utils import table_path, merge_into_delta
from ...utils.logging_config import get_logger
from ...metrics.prometheus_client import (
    pipeline_records_processed, schema_errors, kafka_messages_consumed
)
from ...tracing.otel_setup import get_tracer

BRONZE_BASE = "bronze"
DEAD_LETTER = "dead_letter"


def _add_partition_columns(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("event_ts", F.to_timestamp("timestamp"))
          .withColumn("year", F.year("event_ts").cast(StringType()))
          .withColumn("month", F.lpad(F.month("event_ts").cast(StringType()), 2, "0"))
          .withColumn("day", F.lpad(F.dayofmonth("event_ts").cast(StringType()), 2, "0"))
          .withColumn("ingest_timestamp", F.current_timestamp())
    )


def _validate_payment(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    valid = df.filter(
        F.col("transaction_id").isNotNull()
        & F.col("merchant_id").isNotNull()
        & F.col("amount").isNotNull()
        & (F.col("amount") > 0)
    )
    invalid = df.subtract(valid)
    return valid, invalid


def ingest_from_kafka(spark: SparkSession, run_id: str = "manual") -> dict[str, int]:
    logger = get_logger("bronze_ingestion", run_id)
    tracer = get_tracer()
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    topic = os.getenv("KAFKA_TOPIC_PAYMENTS", "payments.raw")

    with tracer.start_as_current_span("bronze_ingestion") as span:
        span.set_attribute("topic", topic)

        with tracer.start_as_current_span("kafka_consume"):
            raw_df = (
                spark.readStream
                .format("kafka")
                .option("kafka.bootstrap.servers", bootstrap)
                .option("subscribe", topic)
                .option("startingOffsets", "earliest")
                .option("failOnDataLoss", "false")
                .load()
                .selectExpr("CAST(value AS STRING) as raw_json")
            )

        def process_batch(batch_df: DataFrame, batch_id: int) -> None:
            if batch_df.isEmpty():
                return

            parsed = batch_df.select(
                F.from_json(F.col("raw_json"), _infer_schema()).alias("data")
            ).select("data.*")

            with_partitions = _add_partition_columns(parsed)
            valid, invalid = _validate_payment(with_partitions)

            if not invalid.isEmpty():
                cnt = invalid.count()
                schema_errors.labels(table="raw_payments").inc(cnt)
                invalid.write.format("delta").mode("append").save(table_path(BRONZE_BASE, DEAD_LETTER))
                logger.warning("Schema errors routed to dead_letter", extra={"records_count": cnt})

            if not valid.isEmpty():
                cnt = valid.count()
                with tracer.start_as_current_span("delta_write"):
                    valid.write.format("delta").mode("append").partitionBy(
                        "year", "month", "day", "region"
                    ).option("mergeSchema", "true").save(table_path(BRONZE_BASE, "raw_payments"))

                pipeline_records_processed.labels(stage="bronze", table="raw_payments").inc(cnt)
                kafka_messages_consumed.labels(topic=topic).inc(cnt)
                logger.info("Bronze ingest batch complete", extra={"records_count": cnt})

        # CHECKPOINT_BASE defaults to /tmp for local dev.
        # On GCP set it to a GCS path: gs://<bucket>/checkpoints
        checkpoint_base = os.getenv("CHECKPOINT_BASE", "/tmp/checkpoints")
        query = (
            raw_df.writeStream
            .foreachBatch(process_batch)
            .option("checkpointLocation", f"{checkpoint_base}/bronze_{run_id}")
            .trigger(availableNow=True)
            .start()
        )
        query.awaitTermination()

    return {"status": "complete"}


def _infer_schema():
    from pyspark.sql.types import StructType, StructField, StringType, DoubleType
    return StructType([
        StructField("event_type", StringType()),
        StructField("transaction_id", StringType()),
        StructField("merchant_id", StringType()),
        StructField("customer_id", StringType()),
        StructField("amount", DoubleType()),
        StructField("currency", StringType()),
        StructField("gateway", StringType()),
        StructField("region", StringType()),
        StructField("timestamp", StringType()),
        StructField("refund_id", StringType()),
        StructField("original_tx_id", StringType()),
        StructField("refund_amount", DoubleType()),
        StructField("reason_code", StringType()),
        StructField("dispute_id", StringType()),
        StructField("dispute_amount", DoubleType()),
        StructField("status", StringType()),
        StructField("batch_id", StringType()),
        StructField("gross_amount", DoubleType()),
        StructField("fees", DoubleType()),
        StructField("net_amount", DoubleType()),
    ])


def ingest_batch(spark: SparkSession, records: list[dict], run_id: str = "batch") -> int:
    """Ingest a list of event dicts directly into Bronze (for seeding / testing)."""
    from pyspark.sql import Row
    logger = get_logger("bronze_ingestion", run_id)

    df = spark.createDataFrame([Row(**r) for r in records])
    with_partitions = _add_partition_columns(df)

    payments = with_partitions.filter(F.col("event_type") == "payment_created")
    refunds = with_partitions.filter(F.col("event_type") == "refund_initiated")
    chargebacks = with_partitions.filter(F.col("event_type") == "chargeback_filed")
    settlements = with_partitions.filter(F.col("event_type") == "settlement_processed")

    def _write(sdf: DataFrame, table: str) -> int:
        if sdf.rdd.isEmpty():
            return 0
        cnt = sdf.count()
        sdf.write.format("delta").mode("append").partitionBy(
            "year", "month", "day"
        ).option("mergeSchema", "true").save(table_path(BRONZE_BASE, table))
        pipeline_records_processed.labels(stage="bronze", table=table).inc(cnt)
        return cnt

    total = sum([
        _write(payments, "raw_payments"),
        _write(refunds, "raw_refunds"),
        _write(chargebacks, "raw_chargebacks"),
        _write(settlements, "raw_settlements"),
    ])
    logger.info("Bronze batch ingest done", extra={"records_count": total})
    return total
