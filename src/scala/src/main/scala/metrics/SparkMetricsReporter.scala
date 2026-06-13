package metrics

import org.apache.spark.scheduler._

/** Custom SparkListener that reports job/stage metrics to stdout in JSON
  * so Filebeat can ship them to Logstash → Elasticsearch.
  */
class SparkMetricsReporter extends SparkListener {

  private def nowIso: String = java.time.Instant.now().toString

  override def onJobStart(jobStart: SparkListenerJobStart): Unit =
    println(s"""{"timestamp":"$nowIso","level":"INFO","service":"spark_listener","event":"job_start","job_id":${jobStart.jobId}}""")

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
    val result = jobEnd.jobResult match {
      case JobSucceeded    => "succeeded"
      case _: JobFailed    => "failed"
      case _               => "unknown"
    }
    println(s"""{"timestamp":"$nowIso","level":"INFO","service":"spark_listener","event":"job_end","job_id":${jobEnd.jobId},"result":"$result"}""")
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    val info     = stageCompleted.stageInfo
    val duration = info.completionTime.getOrElse(0L) - info.submissionTime.getOrElse(0L)
    val shuffleBytes = info.taskMetrics.shuffleWriteMetrics.bytesWritten
    println(
      s"""{"timestamp":"$nowIso","level":"INFO","service":"spark_listener","event":"stage_complete",""" +
      s""""stage_id":${info.stageId},"duration_ms":$duration,"shuffle_bytes":$shuffleBytes}"""
    )
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    val failed = taskEnd.reason match {
      case Success => false
      case _       => true
    }
    if (failed) {
      println(
        s"""{"timestamp":"$nowIso","level":"WARN","service":"spark_listener","event":"task_failed",""" +
        s""""stage_id":${taskEnd.stageId},"reason":"${taskEnd.reason}"}"""
      )
    }
  }
}
