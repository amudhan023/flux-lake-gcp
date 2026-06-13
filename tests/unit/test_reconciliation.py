import pytest
from pyspark.sql import Row, functions as F

TOLERANCE = 0.01


def _reconcile(payment_amount: float, settled_amount: float | None) -> str:
    if settled_amount is None:
        return "awaiting_settlement"
    disc = abs(payment_amount - settled_amount)
    if disc == 0:
        return "reconciled"
    if disc <= TOLERANCE:
        return "reconciled_within_tolerance"
    return "discrepancy_flagged"


def test_exact_match_reconciled():
    assert _reconcile(100.0, 100.0) == "reconciled"


def test_within_tolerance_reconciled():
    assert _reconcile(100.0, 100.005) == "reconciled_within_tolerance"


def test_outside_tolerance_flagged():
    assert _reconcile(100.0, 99.0) == "discrepancy_flagged"


def test_missing_settlement_awaiting():
    assert _reconcile(100.0, None) == "awaiting_settlement"


def test_reconciliation_logic_via_spark(spark):
    """Validate reconciliation status derivation via Spark SQL expressions."""
    rows = [
        Row(payment_amount=100.0, settled_amount=100.0),
        Row(payment_amount=100.0, settled_amount=100.005),
        Row(payment_amount=100.0, settled_amount=99.0),
        Row(payment_amount=100.0, settled_amount=None),
    ]
    df = spark.createDataFrame(rows)

    result = df.withColumn(
        "disc",
        F.abs(F.col("payment_amount") - F.coalesce(F.col("settled_amount"), F.lit(0)))
    ).withColumn(
        "status",
        F.when(F.col("settled_amount").isNull(), "awaiting_settlement")
         .when(F.col("disc") == 0, "reconciled")
         .when(F.col("disc") <= TOLERANCE, "reconciled_within_tolerance")
         .otherwise("discrepancy_flagged")
    )

    statuses = [r["status"] for r in result.collect()]
    assert statuses == ["reconciled", "reconciled_within_tolerance", "discrepancy_flagged", "awaiting_settlement"]
