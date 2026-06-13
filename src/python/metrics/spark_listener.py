"""Custom Spark listener that pipes Spark metrics to Prometheus.

Attached to SparkContext as a Python-side listener via Py4J.
"""
from pyspark import SparkContext
from .prometheus_client import spark_job_duration, spark_shuffle_bytes, spark_task_failures


class PipelineSparkListener:
    """Listener registered on SparkContext to forward job/stage events."""

    def onJobEnd(self, job_end) -> None:
        job_id = str(job_end.jobId())
        result = job_end.jobResult()
        # Duration is not directly exposed here; timing done via SLATracker per-stage.

    def onStageCompleted(self, stage_completed) -> None:
        info = stage_completed.stageInfo()
        stage_id = str(info.stageId())
        duration_ms = info.completionTime().getOrElse(0) - info.submissionTime().getOrElse(0)
        spark_job_duration.labels(job_id="current", stage=stage_id).observe(duration_ms / 1000)

        metrics = info.taskMetrics()
        if metrics:
            shuffle_bytes = metrics.shuffleWriteMetrics().bytesWritten()
            spark_shuffle_bytes.labels(job_id="current").inc(shuffle_bytes)

    def onTaskEnd(self, task_end) -> None:
        reason = task_end.reason()
        if reason and "Success" not in str(reason):
            stage_id = str(task_end.stageId())
            spark_task_failures.labels(stage=stage_id).inc()


def register_listener(sc: SparkContext) -> None:
    try:
        listener = PipelineSparkListener()
        sc._jsc.sc().addSparkListener(listener)
    except Exception:
        pass
