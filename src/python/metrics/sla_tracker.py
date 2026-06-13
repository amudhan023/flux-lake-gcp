import time
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Generator

from .prometheus_client import (
    pipeline_run_duration,
    pipeline_last_success,
    pipeline_sla,
    pipeline_records_processed,
)


class SLATracker:
    def __init__(self, pipeline_name: str):
        self.pipeline_name = pipeline_name
        self._start_time: float = 0.0
        self.stage_durations: dict[str, float] = {}

    def start(self) -> None:
        self._start_time = time.time()

    def finish(self) -> float:
        elapsed = time.time() - self._start_time
        pipeline_sla.labels(pipeline_name=self.pipeline_name).set(elapsed)
        pipeline_last_success.labels(pipeline_name=self.pipeline_name).set(
            datetime.now(timezone.utc).timestamp()
        )
        return elapsed

    @contextmanager
    def stage(self, stage_name: str) -> Generator[None, None, None]:
        t0 = time.time()
        try:
            yield
        finally:
            elapsed = time.time() - t0
            self.stage_durations[stage_name] = elapsed
            pipeline_run_duration.labels(
                stage=stage_name, pipeline_name=self.pipeline_name
            ).observe(elapsed)

    def record_records(self, stage: str, table: str, count: int) -> None:
        pipeline_records_processed.labels(stage=stage, table=table).inc(count)
