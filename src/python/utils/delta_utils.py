import os
from pyspark.sql import SparkSession, DataFrame
from delta.tables import DeltaTable


S3_BUCKET = os.getenv("S3_BUCKET", "data-lake")
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://minio:9000")


def table_path(layer: str, table: str) -> str:
    return f"s3a://{S3_BUCKET}/{layer}/{table}"


def table_exists(spark: SparkSession, path: str) -> bool:
    try:
        return DeltaTable.isDeltaTable(spark, path)
    except Exception:
        return False


def merge_into_delta(
    spark: SparkSession,
    source_df: DataFrame,
    target_path: str,
    merge_key: str,
    update_columns: list[str] | None = None,
) -> None:
    if not table_exists(spark, target_path):
        source_df.write.format("delta").save(target_path)
        return

    delta_table = DeltaTable.forPath(spark, target_path)
    merge_condition = f"target.{merge_key} = source.{merge_key}"

    if update_columns:
        set_clause = {col: f"source.{col}" for col in update_columns}
    else:
        set_clause = {col: f"source.{col}" for col in source_df.columns}

    (
        delta_table.alias("target")
        .merge(source_df.alias("source"), merge_condition)
        .whenMatchedUpdate(set=set_clause)
        .whenNotMatchedInsertAll()
        .execute()
    )


def get_delta_history(spark: SparkSession, path: str, limit: int = 10) -> DataFrame:
    return DeltaTable.forPath(spark, path).history(limit)
