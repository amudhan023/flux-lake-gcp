#!/usr/bin/env python3
"""Manual pipeline trigger — runs full Bronze → Silver → Gold pipeline."""
import os
import sys
import uuid
from datetime import datetime, timezone

sys.path.insert(0, "/app")

from src.python.utils.spark_session import get_spark_session
from src.python.pipeline.silver.cleanse_transactions import run_silver_cleanse
from src.python.pipeline.silver.reconciliation import run_reconciliation
from src.python.pipeline.gold.daily_aggregations import run_gold_aggregations
from src.python.pipeline.gold.merchant_analytics import run_merchant_analytics
from src.python.pipeline.gold.ml_feature_store import run_feature_store
from src.python.metrics.sla_tracker import SLATracker
from src.python.utils.logging_config import get_logger


def main() -> None:
    run_id = f"run_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}"
    logger = get_logger("run_pipeline", run_id)
    print(f"Starting pipeline run: {run_id}")

    spark = get_spark_session(app_name=f"pipeline_{run_id}")
    tracker = SLATracker(pipeline_name="full_pipeline")
    tracker.start()

    with tracker.stage("silver_cleanse"):
        result = run_silver_cleanse(spark, run_id=run_id)
        print(f"  Silver cleanse: {result['records_processed']:,} records")

    with tracker.stage("reconciliation"):
        recon = run_reconciliation(spark, run_id=run_id)
        print(f"  Reconciliation: {recon['reconciled']:,} reconciled, {recon['discrepancies']} discrepancies")

    with tracker.stage("gold_aggregation"):
        gold = run_gold_aggregations(spark, run_id=run_id)
        print(f"  Gold: {gold['merchant_summary_records']:,} merchant rows, {gold['hourly_volume_records']:,} hourly rows")

    with tracker.stage("merchant_analytics"):
        run_merchant_analytics(spark, run_id=run_id)

    with tracker.stage("ml_feature_store"):
        features = run_feature_store(spark, run_id=run_id)
        print(f"  ML features: {features['customer_features']:,} customers, {features['merchant_features']:,} merchants")

    elapsed = tracker.finish()
    print(f"\nPipeline complete in {elapsed:.1f}s ({elapsed/60:.1f}m)")
    spark.stop()


if __name__ == "__main__":
    main()
