from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType, DecimalType
)

PAYMENT_SCHEMA = StructType([
    StructField("event_type", StringType(), False),
    StructField("transaction_id", StringType(), False),
    StructField("merchant_id", StringType(), False),
    StructField("customer_id", StringType(), False),
    StructField("amount", DoubleType(), False),
    StructField("currency", StringType(), False),
    StructField("gateway", StringType(), True),
    StructField("region", StringType(), False),
    StructField("timestamp", StringType(), False),
])

REFUND_SCHEMA = StructType([
    StructField("event_type", StringType(), False),
    StructField("refund_id", StringType(), False),
    StructField("original_tx_id", StringType(), False),
    StructField("merchant_id", StringType(), False),
    StructField("refund_amount", DoubleType(), False),
    StructField("reason_code", StringType(), True),
    StructField("timestamp", StringType(), False),
])

CHARGEBACK_SCHEMA = StructType([
    StructField("event_type", StringType(), False),
    StructField("dispute_id", StringType(), False),
    StructField("original_tx_id", StringType(), False),
    StructField("merchant_id", StringType(), False),
    StructField("dispute_amount", DoubleType(), False),
    StructField("status", StringType(), True),
    StructField("timestamp", StringType(), False),
])

SETTLEMENT_SCHEMA = StructType([
    StructField("event_type", StringType(), False),
    StructField("batch_id", StringType(), False),
    StructField("merchant_id", StringType(), False),
    StructField("gross_amount", DoubleType(), False),
    StructField("fees", DoubleType(), False),
    StructField("net_amount", DoubleType(), False),
    StructField("timestamp", StringType(), False),
])

VALID_CURRENCIES = {"USD", "EUR", "GBP", "SGD", "JPY", "AUD", "CAD", "CHF"}
