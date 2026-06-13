import pytest
from pyspark.sql import Row, functions as F


def _payments_df(spark, rows):
    return spark.createDataFrame([Row(**r) for r in rows])


def test_daily_merchant_summary_totals(spark):
    """Daily totals must equal hand-calculated expected values."""
    payments = _payments_df(spark, [
        {"merchant_id": "m1", "transaction_id": "t1", "amount_decimal": 100.0,
         "customer_id": "c1", "event_ts": "2026-01-01 10:00:00"},
        {"merchant_id": "m1", "transaction_id": "t2", "amount_decimal": 200.0,
         "customer_id": "c2", "event_ts": "2026-01-01 11:00:00"},
        {"merchant_id": "m2", "transaction_id": "t3", "amount_decimal": 50.0,
         "customer_id": "c1", "event_ts": "2026-01-01 12:00:00"},
    ])
    payments = payments.withColumn("event_ts", F.to_timestamp("event_ts"))

    result = (
        payments.groupBy("merchant_id", F.to_date("event_ts").alias("report_date"))
        .agg(
            F.sum("amount_decimal").alias("total_amount"),
            F.count("transaction_id").alias("transaction_count"),
            F.countDistinct("customer_id").alias("unique_customers"),
        )
    )

    m1 = result.filter(F.col("merchant_id") == "m1").first()
    assert float(m1["total_amount"]) == pytest.approx(300.0)
    assert m1["transaction_count"] == 2
    assert m1["unique_customers"] == 2

    m2 = result.filter(F.col("merchant_id") == "m2").first()
    assert float(m2["total_amount"]) == pytest.approx(50.0)


def test_hourly_volume_bucket_boundaries(spark):
    """Events at UTC hour boundaries must fall in the correct bucket."""
    payments = _payments_df(spark, [
        {"merchant_id": "m1", "transaction_id": "t1", "amount_decimal": 10.0,
         "customer_id": "c1", "currency": "USD", "region": "us-east",
         "event_ts": "2026-01-01 00:59:59"},
        {"merchant_id": "m1", "transaction_id": "t2", "amount_decimal": 20.0,
         "customer_id": "c2", "currency": "USD", "region": "us-east",
         "event_ts": "2026-01-01 01:00:00"},
    ])
    payments = payments.withColumn("event_ts", F.to_timestamp("event_ts"))

    hourly = (
        payments.withColumn("hour", F.date_trunc("hour", "event_ts"))
        .groupBy("hour")
        .agg(F.count("transaction_id").alias("tx_count"))
    )
    rows = {str(r["hour"]): r["tx_count"] for r in hourly.collect()}
    assert rows.get("2026-01-01 00:00:00") == 1
    assert rows.get("2026-01-01 01:00:00") == 1


def test_ml_feature_7d_window_excludes_day8(spark):
    """7-day rolling window must exclude records older than 7 days."""
    from pyspark.sql.window import Window

    payments = _payments_df(spark, [
        {"customer_id": "c1", "transaction_id": "t1", "amount_decimal": 100.0,
         "event_ts": "2026-01-01 10:00:00"},
        {"customer_id": "c1", "transaction_id": "t8", "amount_decimal": 999.0,
         "event_ts": "2025-12-24 10:00:00"},  # day 8 → must be excluded
    ])
    payments = payments.withColumn("event_ts", F.to_timestamp("event_ts"))

    anchor = "2026-01-01"
    recent = payments.filter(
        F.datediff(F.lit(anchor), F.to_date("event_ts")) <= 7
    )
    total_7d = recent.agg(F.sum("amount_decimal")).first()[0]
    assert float(total_7d) == pytest.approx(100.0)


def test_ml_feature_sparse_customer(spark):
    """90-day window on sparse data (only 2 transactions) handled without error."""
    from pyspark.sql.window import Window

    payments = _payments_df(spark, [
        {"customer_id": "c_sparse", "transaction_id": "t1", "amount_decimal": 10.0,
         "event_ts": "2026-01-01 10:00:00"},
        {"customer_id": "c_sparse", "transaction_id": "t2", "amount_decimal": 20.0,
         "event_ts": "2025-12-01 10:00:00"},
    ])
    payments = payments.withColumn("event_ts", F.to_timestamp("event_ts"))

    anchor = "2026-01-01"
    recent = payments.filter(
        F.datediff(F.lit(anchor), F.to_date("event_ts")) <= 90
    )
    result = recent.agg(F.sum("amount_decimal").alias("spend_90d")).first()
    assert float(result["spend_90d"]) == pytest.approx(30.0)
