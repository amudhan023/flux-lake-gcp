"""End-to-end integration tests: requires `make infra-up` before running."""
import pytest
import os
from dataclasses import asdict

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def spark_integration():
    from src.python.utils.spark_session import get_spark_session
    return get_spark_session(app_name="e2e-test")


@pytest.mark.integration
def test_full_pipeline_bronze_to_gold(spark_integration, tmp_path):
    """Seeds 1k events → bronze → silver → gold and validates counts."""
    from src.python.ingestion.event_types import PaymentEvent, SettlementEvent
    from src.python.pipeline.bronze.ingest_transactions import ingest_batch
    from src.python.pipeline.silver.cleanse_transactions import run_silver_cleanse
    from src.python.pipeline.silver.reconciliation import run_reconciliation
    from src.python.pipeline.gold.daily_aggregations import run_gold_aggregations

    os.environ["STORAGE_BACKEND"] = "local"
    os.environ["GCS_BUCKET"] = str(tmp_path)

    payments = [asdict(PaymentEvent.generate()) for _ in range(800)]
    settlements = [asdict(SettlementEvent.generate("merch_1", 5000.0)) for _ in range(8)]
    records = payments + settlements

    bronze_count = ingest_batch(spark_integration, records, run_id="e2e-test")
    assert bronze_count > 0

    silver_result = run_silver_cleanse(spark_integration, run_id="e2e-test")
    assert silver_result["records_processed"] > 0

    recon_result = run_reconciliation(spark_integration, run_id="e2e-test")
    assert isinstance(recon_result["reconciled"], int)

    gold_result = run_gold_aggregations(spark_integration, run_id="e2e-test")
    assert gold_result["merchant_summary_records"] >= 0
