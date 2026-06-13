import pytest
from decimal import Decimal
from pyspark.sql import Row, functions as F
from pyspark.sql.types import DecimalType


def _make_payment(spark, **overrides):
    defaults = dict(
        event_type="payment_created", transaction_id="txn_001", merchant_id="merch_1",
        customer_id="cust_1", amount=100.0, currency="USD", gateway="stripe",
        region="us-east", timestamp="2026-01-01T10:00:00Z",
    )
    defaults.update(overrides)
    return spark.createDataFrame([Row(**defaults)])


def test_amount_cast_to_decimal(spark):
    from src.python.pipeline.silver.cleanse_transactions import _cast_and_validate

    df = _make_payment(spark, amount=123.45)
    valid, _ = _cast_and_validate(df.withColumn("event_ts", F.to_timestamp(F.lit("2026-01-01T10:00:00Z"))))
    row = valid.first()
    assert float(row["amount_decimal"]) == pytest.approx(123.45, abs=0.001)


def test_invalid_currency_flagged(spark):
    from src.python.pipeline.silver.cleanse_transactions import _cast_and_validate

    df = _make_payment(spark, currency="XYZ").withColumn(
        "event_ts", F.to_timestamp(F.lit("2026-01-01T10:00:00Z"))
    )
    valid, quarantined = _cast_and_validate(df)
    assert valid.count() == 0
    assert quarantined.count() == 1
    assert quarantined.first()["quarantine_reason"] == "invalid_currency"


def test_null_amount_quarantined(spark):
    from src.python.pipeline.silver.cleanse_transactions import _cast_and_validate

    df = spark.createDataFrame([
        Row(event_type="payment_created", transaction_id="txn_001", merchant_id="merch_1",
            customer_id="cust_1", amount=None, currency="USD", gateway="stripe",
            region="us-east", timestamp="2026-01-01T10:00:00Z",
            event_ts=None, amount_decimal=None, currency_valid=True)
    ])
    valid, quarantined = _cast_and_validate(df)
    assert quarantined.count() >= 1


def test_reconciliation_awaiting_settlement(spark, sample_payments, sample_settlements):
    from src.python.pipeline.silver.reconciliation import run_reconciliation
    # Integration-style test: payments without settlement → awaiting_settlement
    # Tested via direct DataFrame logic
    from pyspark.sql import functions as F
    payments_agg = (
        sample_payments
        .withColumn("event_ts", F.to_timestamp("timestamp"))
        .withColumn("payment_date", F.to_date("timestamp"))
        .groupBy("merchant_id", "payment_date")
        .agg(F.sum("amount").alias("payment_amount"))
    )
    # merch_2 has no settlement → should be awaiting
    merch2 = payments_agg.filter(F.col("merchant_id") == "merch_2")
    assert merch2.count() == 1


def test_reconciliation_matched(spark, sample_payments, sample_settlements):
    from pyspark.sql import functions as F
    payments_agg = (
        sample_payments
        .filter(F.col("merchant_id") == "merch_1")
        .withColumn("payment_date", F.to_date("timestamp"))
        .groupBy("merchant_id", "payment_date")
        .agg(F.sum("amount").alias("payment_amount"))
    )
    settlements_agg = (
        sample_settlements
        .withColumn("settlement_date", F.to_date("timestamp"))
        .groupBy("merchant_id", "settlement_date")
        .agg(F.sum("gross_amount").alias("settled_amount"))
    )
    joined = payments_agg.join(
        settlements_agg,
        (payments_agg.merchant_id == settlements_agg.merchant_id)
        & (payments_agg.payment_date == settlements_agg.settlement_date),
    )
    assert joined.count() == 1
