import pytest
import time
from unittest.mock import patch


def test_sla_tracker_measures_duration():
    from src.python.metrics.sla_tracker import SLATracker

    tracker = SLATracker("test_pipeline")
    tracker.start()

    with tracker.stage("stage_a"):
        time.sleep(0.05)

    elapsed = tracker.finish()
    assert elapsed >= 0.05
    assert "stage_a" in tracker.stage_durations
    assert tracker.stage_durations["stage_a"] >= 0.05


def test_sla_tracker_records_records():
    from src.python.metrics.sla_tracker import SLATracker
    from src.python.metrics.prometheus_client import pipeline_records_processed

    tracker = SLATracker("test_pipeline_2")
    before = pipeline_records_processed.labels(stage="bronze", table="test_table")._value.get()
    tracker.record_records("bronze", "test_table", 500)
    after = pipeline_records_processed.labels(stage="bronze", table="test_table")._value.get()
    assert (after - before) == 500.0


def test_schema_error_counter_increments():
    from src.python.metrics.prometheus_client import schema_errors

    before = schema_errors.labels(table="raw_payments")._value.get()
    schema_errors.labels(table="raw_payments").inc(5)
    after = schema_errors.labels(table="raw_payments")._value.get()
    assert (after - before) == 5.0


def test_pipeline_last_success_updates():
    from src.python.metrics.prometheus_client import pipeline_last_success
    import time

    pipeline_last_success.labels(pipeline_name="test_pl").set(time.time())
    val = pipeline_last_success.labels(pipeline_name="test_pl")._value.get()
    assert val > 0
