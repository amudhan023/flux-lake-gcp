"""Ingest static reference data (merchant catalogue) into Bronze."""
import os
import random
from pyspark.sql import SparkSession, Row
from pyspark.sql import functions as F

from ...utils.delta_utils import table_path


def seed_merchant_reference(spark: SparkSession, merchant_count: int = 500) -> int:
    merchants = [
        Row(
            merchant_id=f"merch_{i}",
            merchant_name=f"Merchant {i}",
            category=random.choice(["retail", "food", "travel", "electronics", "services"]),
            country=random.choice(["US", "GB", "DE", "SG", "AU"]),
            risk_tier=random.choice(["low", "medium", "high"]),
        )
        for i in range(1, merchant_count + 1)
    ]
    df = spark.createDataFrame(merchants)
    df.write.format("delta").mode("overwrite").save(
        table_path("bronze", "merchant_reference")
    )
    return merchant_count
