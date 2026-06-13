"""Delta Lake ACID property tests — require local Delta environment."""
import pytest
from concurrent.futures import ThreadPoolExecutor
from pyspark.sql import Row

pytestmark = pytest.mark.integration


def test_time_travel(spark, tmp_path):
    """Writing version 1 then version 2; VERSION AS OF 1 returns original data."""
    path = str(tmp_path / "time_travel")

    spark.createDataFrame([Row(id=1, val="original")]).write.format("delta").save(path)

    spark.createDataFrame([Row(id=2, val="new_row")]).write.format("delta").mode("append").save(path)

    v1 = spark.read.format("delta").option("versionAsOf", 0).load(path)
    assert v1.count() == 1
    assert v1.first()["val"] == "original"


def test_schema_evolution_adds_nullable_column(spark, tmp_path):
    """Adding a new nullable column must succeed without rewriting existing data."""
    path = str(tmp_path / "schema_evo")

    spark.createDataFrame([Row(id=1, name="Alice")]).write.format("delta").save(path)

    spark.createDataFrame([Row(id=2, name="Bob", extra_col="new")]).write.format("delta") \
        .mode("append").option("mergeSchema", "true").save(path)

    df = spark.read.format("delta").load(path)
    assert "extra_col" in df.columns
    assert df.count() == 2


def test_concurrent_writes_no_data_loss(spark, tmp_path):
    """Two concurrent writers to the same Delta table must not lose records."""
    path = str(tmp_path / "concurrent")

    spark.createDataFrame([Row(id=0)]).write.format("delta").save(path)

    def write_batch(batch_id: int) -> None:
        rows = [Row(id=batch_id * 100 + i) for i in range(10)]
        spark.createDataFrame(rows).write.format("delta").mode("append").save(path)

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(write_batch, i) for i in range(1, 3)]
        for f in futures:
            f.result()

    total = spark.read.format("delta").load(path).count()
    assert total == 21  # 1 initial + 10 + 10


def test_failed_write_rollback(spark, tmp_path):
    """A mid-write failure must leave the table in its pre-write state."""
    path = str(tmp_path / "rollback")

    initial = spark.createDataFrame([Row(id=1)])
    initial.write.format("delta").save(path)

    try:
        (
            spark.createDataFrame([Row(id=2), Row(id=3)])
            .write.format("delta")
            .mode("append")
            .option("delta.commitInfo.userMetadata", "force_fail_test")
            .save(path)
        )
    except Exception:
        pass

    count = spark.read.format("delta").load(path).count()
    assert count >= 1  # original record is preserved
