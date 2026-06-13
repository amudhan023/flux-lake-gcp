"""FastAPI service exposing /metrics (Prometheus) and /trigger (pipeline run)."""
import os
import uuid
import time
from datetime import datetime, timezone

from fastapi import FastAPI, BackgroundTasks
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
from fastapi.responses import Response

from .utils.spark_session import get_spark_session
from .utils.logging_config import get_logger
from .pipeline.bronze.ingest_transactions import ingest_batch
from .pipeline.silver.cleanse_transactions import run_silver_cleanse
from .pipeline.silver.reconciliation import run_reconciliation
from .pipeline.gold.daily_aggregations import run_gold_aggregations
from .pipeline.gold.merchant_analytics import run_merchant_analytics
from .pipeline.gold.ml_feature_store import run_feature_store
from .metrics.sla_tracker import SLATracker
from .tracing.otel_setup import init_tracer

app = FastAPI(title="Pipeline API", version="1.0.0")
init_tracer("pipeline-api")

_pipeline_status: dict = {"status": "idle", "last_run_id": None, "last_run_at": None}


def _run_full_pipeline(run_id: str) -> None:
    global _pipeline_status
    logger = get_logger("pipeline_api", run_id)
    _pipeline_status = {"status": "running", "run_id": run_id, "started_at": datetime.now(timezone.utc).isoformat()}

    tracker = SLATracker(pipeline_name="full_pipeline")
    tracker.start()

    try:
        spark = get_spark_session(app_name=f"pipeline_{run_id}")

        with tracker.stage("silver_cleanse"):
            result = run_silver_cleanse(spark, run_id=run_id)
            tracker.record_records("silver", "cleansed_transactions", result["records_processed"])

        with tracker.stage("reconciliation"):
            run_reconciliation(spark, run_id=run_id)

        with tracker.stage("gold_aggregation"):
            run_gold_aggregations(spark, run_id=run_id)

        with tracker.stage("merchant_analytics"):
            run_merchant_analytics(spark, run_id=run_id)

        with tracker.stage("ml_feature_store"):
            run_feature_store(spark, run_id=run_id)

        elapsed = tracker.finish()
        _pipeline_status = {
            "status": "complete",
            "run_id": run_id,
            "duration_s": elapsed,
            "completed_at": datetime.now(timezone.utc).isoformat(),
        }
        logger.info("Pipeline complete", extra={"records_count": 0, "duration_ms": int(elapsed * 1000)})

    except Exception as exc:
        _pipeline_status = {"status": "failed", "run_id": run_id, "error": str(exc)}
        logger.error(f"Pipeline failed: {exc}")
        raise


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/status")
def status():
    return _pipeline_status


@app.post("/trigger")
def trigger(background_tasks: BackgroundTasks):
    run_id = f"run_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}"
    background_tasks.add_task(_run_full_pipeline, run_id)
    return {"message": "Pipeline triggered", "run_id": run_id}


@app.get("/metrics")
def metrics():
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)
