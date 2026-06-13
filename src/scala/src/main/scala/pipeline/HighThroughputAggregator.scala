package pipeline

import io.delta.tables.DeltaTable
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._

/** Cross-merchant daily reconciliation aggregator.
  *
  * Executed as the hot-path shuffle job from the Python orchestrator:
  *   spark-submit --class pipeline.HighThroughputAggregator pipeline-scala.jar <s3_base> <report_date>
  *
  * Using Dataset API (not DataFrame) for compile-time type safety and lower
  * Python-UDF serialization overhead on the critical shuffle path.
  */
object HighThroughputAggregator {

  case class Transaction(
      transaction_id: String,
      merchant_id: String,
      customer_id: String,
      amount_decimal: Double,
      currency: String,
      region: String,
      event_ts: String,
  )

  case class MerchantDailySummary(
      merchant_id: String,
      report_date: String,
      total_amount: Double,
      transaction_count: Long,
      avg_amount: Double,
      unique_customers: Long,
      currency: String,
  )

  def run(spark: SparkSession, s3Base: String, reportDate: String): Long = {
    import spark.implicits._

    val silverPath = s"$s3Base/silver/cleansed_transactions"
    val goldPath   = s"$s3Base/gold/daily_merchant_summary_scala"

    val transactions: Dataset[Transaction] = spark.read
      .format("delta")
      .load(silverPath)
      .where(s"date(event_ts) = '$reportDate'")
      .as[Transaction]

    val summary: Dataset[MerchantDailySummary] = transactions
      .groupByKey(_.merchant_id)
      .mapGroups { (merchantId, rows) =>
        val batch = rows.toList
        val totalAmt   = batch.map(_.amount_decimal).sum
        val txCount    = batch.size.toLong
        val uniqueCust = batch.map(_.customer_id).distinct.size.toLong
        val topCurrency = batch.groupBy(_.currency).maxBy(_._2.size)._1
        MerchantDailySummary(
          merchant_id       = merchantId,
          report_date       = reportDate,
          total_amount      = totalAmt,
          transaction_count = txCount,
          avg_amount        = if (txCount > 0) totalAmt / txCount else 0.0,
          unique_customers  = uniqueCust,
          currency          = topCurrency,
        )
      }

    val count = summary.count()

    summary.write
      .format("delta")
      .mode("overwrite")
      .option("replaceWhere", s"report_date = '$reportDate'")
      .save(goldPath)

    count
  }

  def main(args: Array[String]): Unit = {
    require(args.length >= 2, "Usage: HighThroughputAggregator <s3_base> <report_date>")

    val spark = SparkSession.builder()
      .appName("HighThroughputAggregator")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()

    val count = run(spark, args(0), args(1))
    println(s"[HighThroughputAggregator] Written $count merchant summaries for ${args(1)}")
    spark.stop()
  }
}
