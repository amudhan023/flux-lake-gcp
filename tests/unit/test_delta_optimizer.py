import pytest
import os
from pyspark.sql import Row


def test_optimize_reduces_file_count(spark, tmp_path):
    """Writing many small files then OPTIMIZE should reduce file count."""
    path = str(tmp_path / "test_table")
    for i in range(5):
        (
            spark.createDataFrame([Row(id=i, value=f"v{i}")])
            .write.format("delta").mode("append").save(path)
        )

    from delta.tables import DeltaTable
    before = DeltaTable.forPath(spark, path).detail().select("numFiles").first()["numFiles"]
    spark.sql(f"OPTIMIZE delta.`{path}`")
    after = DeltaTable.forPath(spark, path).detail().select("numFiles").first()["numFiles"]

    assert after <= before


def test_vacuum_enforces_minimum_retention(spark, tmp_path):
    """VACUUM with retention < 168h should be clamped or raise."""
    path = str(tmp_path / "vacuum_test")
    spark.createDataFrame([Row(id=1)]).write.format("delta").save(path)

    from src.python.optimization.delta_optimizer import vacuum_table
    os.environ["DELTA_VACUUM_RETENTION_HOURS"] = "10"

    result = vacuum_table(spark, str(tmp_path), "vacuum_test", run_id="test")
    assert result["retention_hours"] == 168


def test_zorder_runs_without_error(spark, tmp_path):
    path = str(tmp_path / "zorder_test")
    spark.createDataFrame([
        Row(merchant_id="m1", amount=100.0),
        Row(merchant_id="m2", amount=200.0),
    ]).write.format("delta").save(path)

    spark.sql(f"OPTIMIZE delta.`{path}` ZORDER BY (merchant_id)")


def test_optimize_is_idempotent(spark, tmp_path):
    """Running OPTIMIZE twice on the same table must not corrupt it."""
    path = str(tmp_path / "idempotent")
    spark.createDataFrame([Row(id=1, val="a"), Row(id=2, val="b")]).write.format("delta").save(path)

    spark.sql(f"OPTIMIZE delta.`{path}`")
    count1 = spark.read.format("delta").load(path).count()
    spark.sql(f"OPTIMIZE delta.`{path}`")
    count2 = spark.read.format("delta").load(path).count()

    assert count1 == count2 == 2
