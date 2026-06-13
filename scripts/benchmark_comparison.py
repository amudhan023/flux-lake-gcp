#!/usr/bin/env python3
"""Benchmark: runs slow vs optimized pipeline and outputs SLA comparison."""
import os
import sys
import json
import time
import uuid
from datetime import datetime, timezone

sys.path.insert(0, "/app")

from src.python.utils.spark_session import get_spark_session
from src.python.pipeline.silver.cleanse_transactions import run_silver_cleanse
from src.python.pipeline.silver.reconciliation import run_reconciliation
from src.python.pipeline.gold.daily_aggregations import run_gold_aggregations
from src.python.metrics.sla_tracker import SLATracker


STAGES = ["silver_cleanse", "reconciliation", "gold_aggregation"]


def _run_pipeline(spark, run_id: str) -> dict[str, float]:
    durations: dict[str, float] = {}
    tracker = SLATracker(pipeline_name=run_id)
    tracker.start()

    with tracker.stage("silver_cleanse"):
        run_silver_cleanse(spark, run_id=run_id)

    with tracker.stage("reconciliation"):
        run_reconciliation(spark, run_id=run_id)

    with tracker.stage("gold_aggregation"):
        run_gold_aggregations(spark, run_id=run_id)

    tracker.finish()
    return tracker.stage_durations


def main() -> None:
    print("Starting SLA benchmark comparison...\n")

    spark = get_spark_session(app_name="benchmark")

    # ── Slow pipeline ─────────────────────────────────────────────────────────
    os.environ["ENABLE_DELTA_OPTIMIZE"] = "false"
    os.environ["ENABLE_ZORDER"] = "false"
    os.environ["ENABLE_AQE"] = "false"
    os.environ["ENABLE_BROADCAST_JOINS"] = "false"
    os.environ["ENABLE_INCREMENTAL_GOLD"] = "false"

    print("Running SLOW baseline pipeline...")
    slow_id = f"slow_{uuid.uuid4().hex[:6]}"
    slow_durations = _run_pipeline(spark, slow_id)

    # ── Optimized pipeline ────────────────────────────────────────────────────
    os.environ["ENABLE_DELTA_OPTIMIZE"] = "true"
    os.environ["ENABLE_ZORDER"] = "true"
    os.environ["ENABLE_AQE"] = "true"
    os.environ["ENABLE_BROADCAST_JOINS"] = "true"
    os.environ["ENABLE_INCREMENTAL_GOLD"] = "true"

    print("Running OPTIMIZED pipeline...")
    opt_id = f"opt_{uuid.uuid4().hex[:6]}"
    opt_durations = _run_pipeline(spark, opt_id)

    # ── Report ────────────────────────────────────────────────────────────────
    print(f"\n{'Stage':<30} {'Slow':>12} {'Optimized':>12} {'Improvement':>12}")
    print("─" * 70)

    results = {}
    slow_total = sum(slow_durations.values())
    opt_total = sum(opt_durations.values())

    for stage in STAGES:
        slow_s = slow_durations.get(stage, 0)
        opt_s = opt_durations.get(stage, 0)
        improvement = (1 - opt_s / slow_s) * 100 if slow_s > 0 else 0
        print(f"{stage:<30} {slow_s/60:>10.1f}m {opt_s/60:>10.1f}m {improvement:>10.0f}%")
        results[stage] = {"slow_s": slow_s, "optimized_s": opt_s, "improvement_pct": improvement}

    improvement_total = (1 - opt_total / slow_total) * 100 if slow_total > 0 else 0
    print("─" * 70)
    print(f"{'TOTAL':<30} {slow_total/3600:>9.2f}h {opt_total/3600:>9.2f}h {improvement_total:>10.0f}%")

    output = {
        "run_at": datetime.now(timezone.utc).isoformat(),
        "stages": results,
        "totals": {
            "slow_s": slow_total,
            "optimized_s": opt_total,
            "improvement_pct": improvement_total,
        }
    }
    with open("benchmark_results.json", "w") as f:
        json.dump(output, f, indent=2)

    print("\nResults saved to benchmark_results.json")
    spark.stop()


if __name__ == "__main__":
    main()
