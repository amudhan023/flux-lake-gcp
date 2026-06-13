import os
import pytest
from pyspark.sql import SparkSession

os.environ.setdefault("AWS_ACCESS_KEY_ID", "minioadmin")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "minioadmin123")
os.environ.setdefault("MINIO_ENDPOINT", "http://localhost:9000")
os.environ.setdefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
os.environ.setdefault("ENABLE_DELTA_OPTIMIZE", "true")
os.environ.setdefault("ENABLE_ZORDER", "false")  # skip in tests
os.environ.setdefault("ENABLE_AQE", "true")
os.environ.setdefault("ENABLE_BROADCAST_JOINS", "true")
os.environ.setdefault("ENABLE_INCREMENTAL_GOLD", "true")
os.environ.setdefault("S3_BUCKET", "data-lake")


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    return (
        SparkSession.builder.appName("pipeline-tests")
        .master("local[2]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.default.parallelism", "4")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
        .getOrCreate()
    )


@pytest.fixture(scope="session")
def delta_base(tmp_path_factory) -> str:
    return str(tmp_path_factory.mktemp("delta"))


@pytest.fixture
def sample_payments(spark):
    from pyspark.sql import Row
    return spark.createDataFrame([
        Row(event_type="payment_created", transaction_id="txn_001", merchant_id="merch_1",
            customer_id="cust_1", amount=100.0, currency="USD", gateway="stripe",
            region="us-east", timestamp="2026-01-01T10:00:00Z"),
        Row(event_type="payment_created", transaction_id="txn_002", merchant_id="merch_1",
            customer_id="cust_2", amount=200.0, currency="EUR", gateway="adyen",
            region="eu-west", timestamp="2026-01-01T11:00:00Z"),
        Row(event_type="payment_created", transaction_id="txn_003", merchant_id="merch_2",
            customer_id="cust_1", amount=50.0, currency="GBP", gateway="braintree",
            region="eu-west", timestamp="2026-01-01T12:00:00Z"),
    ])


@pytest.fixture
def sample_settlements(spark):
    from pyspark.sql import Row
    return spark.createDataFrame([
        Row(event_type="settlement_processed", batch_id="batch_001", merchant_id="merch_1",
            gross_amount=300.0, fees=9.0, net_amount=291.0, timestamp="2026-01-01T23:00:00Z"),
    ])
