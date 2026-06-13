"""Partition strategy helpers — repartition and coalesce for optimal file sizes."""
import os
from pyspark.sql import DataFrame

TARGET_FILE_SIZE_MB = int(os.getenv("TARGET_FILE_SIZE_MB", "128"))
SHUFFLE_PARTITIONS = int(os.getenv("SPARK_SHUFFLE_PARTITIONS", "200"))


def repartition_for_write(df: DataFrame, estimated_rows: int) -> DataFrame:
    """Repartition to hit ~TARGET_FILE_SIZE_MB per output file."""
    estimated_mb = estimated_rows * 0.001
    target_files = max(1, int(estimated_mb / TARGET_FILE_SIZE_MB))
    return df.repartition(target_files)


def coalesce_for_write(df: DataFrame, max_files: int = 50) -> DataFrame:
    """Coalesce partitions down without a full shuffle."""
    return df.coalesce(max_files)


def optimal_partition_count(spark_context) -> int:
    """Derive shuffle partition count from cluster parallelism."""
    cores = spark_context.defaultParallelism
    return min(max(cores * 2, 10), SHUFFLE_PARTITIONS)
