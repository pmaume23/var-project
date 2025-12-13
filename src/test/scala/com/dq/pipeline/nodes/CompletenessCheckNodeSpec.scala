package com.dq.pipeline.Nodes

import com.dq.pipeline.testutil.SparkTestSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.scalatest.funsuite.AnyFunSuite
import org.slf4j.{Logger, LoggerFactory}

class CompletenessCheckNodeSpec extends AnyFunSuite with SparkTestSession {

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  test("completeness report counts pass and fail rows per column") {
    logger.info("Testing completeness report generation")
    import spark.implicits._
    val schema = StructType(Seq(
      StructField("Ticker", StringType, nullable = true),
      StructField("Date", StringType, nullable = true),
      StructField("Close", DoubleType, nullable = true)
    ))

    val rows = Seq(
      Row("AAA", "2020-01-01", 10.0),
      Row("BBB", null, null)
    )

    val df = spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)
    val report = new CompletenessCheckNode(df).completenessReportDF()

    val ticker = report.filter($"LogicalAttribute" === "Ticker").head()
    val date = report.filter($"LogicalAttribute" === "Date").head()
    val close = report.filter($"LogicalAttribute" === "Close").head()

    assert(ticker.getAs[Long]("Pass") == 2L && ticker.getAs[Long]("Fail") == 0L)
    assert(date.getAs[Long]("Pass") == 1L && date.getAs[Long]("Fail") == 1L)
    assert(close.getAs[Long]("Pass") == 1L && close.getAs[Long]("Fail") == 1L)
    logger.info("Completeness report test passed")
  }
}
